#include "consistent.hpp"

#include <iostream>

class TestHasher {
public:
	uint64_t Sum64(void const* data, size_t size) const{
		return std::hash<std::string>{}(std::string((char const*)data, size));
	}
};

class ServerNode {
	std::string name_;
public:
	ServerNode(std::string& name = std::string("")) : name_(name) {}
	std::string String()const {
		return name_;
	}
};

int main(){
	using namespace consistent;

	Config<>{ Config<>::Hasher(), 71, 30, 1.5, };
	auto config = Config<TestHasher>{ TestHasher(), 71, 30, 1.5, };

	std::vector<ServerNode> members;
	for (int i = 0; i < 10; ++i) { 
		char buf[128];
		sprintf(buf, "node%d.mycompany.com:port%d", i, i);
		members.push_back(ServerNode(std::string(buf)));
	}
	Consistent<Config<TestHasher>, ServerNode> c(members, config);

	auto PrintMember = [](ServerNode const& member) {std::cout << member.String() << std::endl;};
	std::string key("my-key");
	ServerNode member;
	if (c.LocateKey(key, member)) {
		PrintMember(member);
	}
	else {
		assert(false);
	}

	std::vector<ServerNode> members1;
	c.GetClosestN(key, 2, members1);
	for (auto const& member : members1)
		PrintMember(member);
	c.GetMembers();
	c.AverageLoad();
	assert( ! c.Add(member));
	c.Remove("node9.mycompany.port9");
	c.LoadDistribution();
	std::cout << "key1's partitionId=" << c.FindPartitionID("key1") << std::endl;
	c.GetPartitionOwner(2, member);

	members1.clear();
	c.GetClosestNForPartition(3, 2, members1);
	for (auto const& member : members1)
		PrintMember(member);
	return 0;
}