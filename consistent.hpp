// Copyright (c) 2018 Burak Sezer
// All rights reserved.
//
// This code is licensed under the MIT License.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files(the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and / or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions :
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Package consistent provides a consistent hashing function with bounded loads.
// For more information about the underlying algorithm, please take a look at
// https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html
//
// Example Use:
// 	cfg := consistent.Config{
// 		PartitionCount:    71,
// 		ReplicationFactor: 20,
// 		Load:              1.25,
// 		Hasher:            hasher{},
//	}
//
//      // Create a new consistent object
//      // You may call this with a list of members
//      // instead of adding them one by one.
//	c := consistent.New(members, cfg)
//
//      // myMember struct just needs to implement a String method.
//      // New/Add/Remove distributes partitions among members using the algorithm
//      // defined on Google Research Blog.
//	c.Add(myMember)
//
//	key := []byte("my-key")
//      // LocateKey hashes the key and calculates partition ID with
//      // this modulo operation: MOD(hash result, partition count)
//      // The owner of the partition is already calculated by New/Add/Remove.
//      // LocateKey just returns the member which's responsible for the key.
//	member := c.LocateKey(key)
//

// 有界负载的一致性哈希算法 https://www.leiphone.com/news/201704/xRiQjGMRKnJaXuOe.html
// http://liblb.com/bounded.html

#pragma once

#include "WfirstRWLock.hpp"
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <map>
#include <sstream>
#include <vector>

namespace consistent {
	
	//ErrInsufficientMemberCount represents an error which means there are not enough members to complete the task.
	//char const*	ErrInsufficientMemberCount = "insufficient member count";

	// ErrMemberNotFound represents an error which means requested member could not be found in consistent hash ring.
	//char const*	ErrMemberNotFound = "member could not be found in ring";
	enum class Error {
		ErrorOk,
		ErrInsufficientMemberCount,
		ErrMemberNotFound,
	};
	/// Config represents a structure to control consistent package.

	// Hasher is responsible for generating unsigned, 64 bit hash of provided byte slice.
	// Hasher should minimize collisions (generating same hash for different byte slice)
	// and while performance is also important fast functions are preferable (i.e.
	// you can use FarmHash family).
	// Hasher 必须有函数 uint64_t Sum64(void const* data, size_t size)
	template<typename Hasher>
	class Config {
	public:
		typedef typename Hasher Hasher;
		// Hasher is responsible for generating unsigned, 64 bit hash of provided byte slice.
		Hasher hasher;

		// Keys are distributed among partitions. Prime numbers are good to
		// distribute keys uniformly. Select a big PartitionCount if you have
		// too many keys.
		int PartitionCount = 71;

		// Members are replicated on consistent hash ring. This number means that a member
		// how many times replicated on the ring.
		int ReplicationFactor = 20;

		// Load is used to calculate average load. See the code, the paper and Google's blog post to learn about it.
		double Load = 1.25;
	};
	
	// Consistent holds the information about the members of the consistent hash circle.
	// Member interface represents a member in consistent hash ring.
	// Member 需要有函数std::string String()const;
	template<typename Config, typename Member>
	class Consistent {
		typedef typename Config::Hasher Hasher;
		WfirstRWLock mu;

		Config config;
		Hasher hasher;
		std::vector<uint64_t> sortedSet;
		uint64_t partitionCount;
		std::map<std::string, double>	loads;
		std::map<std::string, Member>	members;
		std::map<int, Member*>			partitions;
		std::map <uint64_t, Member*>	ring;
	public:
		// New creates and returns a new Consistent object.
		Consistent(std::vector<Member> const& _members, Config const& _config) 
			: config(_config)
			, partitionCount(config.PartitionCount)
			, hasher(config.hasher) {

			// TODO: Check configuration here
;
			for (auto const& member : _members) {
				add(member);
			}
			if (!_members.empty()) {
				distributePartitions();
			}
		}

		// GetMembers returns a thread-safe copy of members.
		std::vector<Member> GetMembers()  {
			unique_readguard<WfirstRWLock> lock(mu);

			// Create a thread-safe copy of member list.
			std::vector<Member> result;
			for (auto const& pair : members)
				result.push_back(pair.second);
			return result;
		}
		// AverageLoad exposes the current average load.
		double AverageLoad()const {
			double avgLoad = double(partitionCount / members.size()) * config.Load;
			return ceil(avgLoad);
		}

		// Add adds a new member to the consistent hash circle.
		bool Add(Member const& member) {
			unique_writeguard<WfirstRWLock> lock(mu);

			auto it = members.find(member.String());
			if (it != members.end()) {
				// We have already have this. Quit immediately.
				return false;
			}
			add(member);
			distributePartitions();
			return true;
		}

		// Remove removes a member from the consistent hash circle.
		bool Remove(std::string const& name) {
			unique_writeguard<WfirstRWLock> lock(mu);
			auto it = members.find(name);
			if (it != members.end()) {
				// There is no member with that name. Quit immediately.
				return false;
			}

			for (int i = 0; i < config.ReplicationFactor; ++i) {
				char key[128];
				sprintf(key, "%s%d", name.c_str(), i);
				auto h =hasher.Sum64(key, strlen(key));
				ring.erase(h);
				delSlice(h);
			}
			members.erase(name);
			if (members.empty()) {
				// consistent hash ring is empty now. Reset the partition table.
				partitions.clear();
				return true;
			}
			distributePartitions();
			return true;
		}

		// LoadDistribution exposes load distribution of members.
		std::map<std::string, double> LoadDistribution()  {
			unique_readguard<WfirstRWLock> lock(mu);

			// Create a thread-safe copy
			return loads;
		}

		// FindPartitionID returns partition id for given key.
		int FindPartitionID(std::string const& key) const {
			auto hkey = hasher.Sum64(key.c_str(), key.length());
			return int(hkey % partitionCount);
		}

		// GetPartitionOwner returns the owner of the given partition.
		bool GetPartitionOwner(int partID, Member& member)  {
			unique_readguard<WfirstRWLock> lock(mu);

			auto it = partitions.find(partID);
			if (it == partitions.end()) {
				return false;
			}
			// Create a thread-safe copy of member and return it. 
			member = *it->second;
			return true;
		}

		// LocateKey finds a home for given key
		bool LocateKey(std::string const& key, Member& member) {
			auto partID = FindPartitionID(key);
			return GetPartitionOwner(partID, member);
		}


		// GetClosestN returns the closest N member to a key in the hash ring. This may be useful to find members for replication.
		Error GetClosestN(std::string const& key, int count, std::vector<Member>& res)  {
			auto partID = FindPartitionID(key);
			return getClosestN(partID, count, res);
		}
		// GetClosestNForPartition returns the closest N member for given partition. This may be useful to find members for replication.
		Error GetClosestNForPartition(int partID, int count, std::vector<Member>& res)  {
			return getClosestN(partID, count, res);
		}

	private:
		/// @param idx sortedSet的下标
		void distributeWithLoad(int partID, int idx, std::map<int, Member*>& partitions, std::map<std::string, double>& loads) {
			auto avgLoad = AverageLoad();
			int count = 0;
			for (;;) {
				if (++count >= sortedSet.size()) {
					// User needs to decrease partition count, increase member count or increase load factor.
					assert(false && "not enough room to distribute partitions");
					return;
				}
				auto i = sortedSet[idx];
				auto member = ring[i];
				assert(member);
				auto member_name = member->String();
				auto load = loads[member_name];
				if (load + 1 <= avgLoad) {
					partitions[partID] = member;
					++loads[member_name];
					return;
				}
				
				if (++idx >= sortedSet.size()) {
					idx = 0;
				}
			}
		}

		void distributePartitions() {
			std::map<std::string, double> loads;
			std::map<int, Member*> partitions;

			for (uint64_t partID = 0; partID < partitionCount; ++partID) {				
				auto key = hasher.Sum64(&partID, sizeof(partID));
				auto be = sortedSet.begin(), en = sortedSet.end();
				auto it = std::lower_bound(be, en, key);
				auto idx = it == en ? 0 : it - be;
				
				distributeWithLoad(int(partID), idx, partitions, loads);
			}
			std::swap(this->partitions, partitions);
			std::swap(this->loads, loads);
		}

		void add(Member const& member ) {
			// Storing member at this map is useful to find backup members of a partition.
			auto member_name = member.String();
			members.insert(std::make_pair(member_name, member));
			auto pmember = &members[member_name];
			for (int i = 0; i < config.ReplicationFactor; ++i) {
				std::stringstream ss;
				ss << member.String() << i;
				std::string key = ss.str();
				auto h = hasher.Sum64(key.data(), key.length());
				ring[h] = pmember;
				sortedSet.push_back(h);
			}
			// sort hashes ascendingly
			std::sort(sortedSet.begin(), sortedSet.end());			
		}
			
		void delSlice(uint64_t val) {
			sortedSet.erase(std::remove(sortedSet.begin(), sortedSet.end(), val), sortedSet.end());
		}

		Error getClosestN(int partID, int count, std::vector<Member>& res)  {
			unique_readguard<WfirstRWLock> lock(mu);

			res.clear();
			if (count > members.size() - 1) {
				return Error::ErrInsufficientMemberCount;
			}

			uint64_t ownerKey;
			Member owner;
			if (!GetPartitionOwner(partID, owner)) {
				return Error::ErrMemberNotFound;
			}
			// Hash and sort all the names.
			std::vector<uint64_t> keys;
			std::map<uint64_t, Member const*> kmems;
			for (auto it = members.begin(), en = members.end(); it != en; ++it) {
				std::string const& name = it->first; 
				Member const& member = it->second;
				auto key = hasher.Sum64(name.c_str(), name.length());
				if (name == owner.String()) {
					ownerKey = key;
				}
				keys.push_back(key);
				kmems[key] = &member;
			}
			std::sort(keys.begin(), keys.end());

			// Find the member
			size_t idx = 0;
			while (idx < keys.size()) {
				if (keys[idx] == ownerKey){
					break;
				}
				idx++;
			}

			// Find the closest members.
			while (res.size() < count) {
				idx++;
				if (idx >= keys.size()) {
					idx = 0;
				}
				auto key = keys[idx];
				res.push_back(*kmems[key]);
			}
			return Error::ErrorOk;
		}

	};
	
}
