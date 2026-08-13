#include <iostream>
#include <string>

#include "proto/calculator.pb.h"

int main()
{
   //1.客户端创建请求对象
	minirpc::AddRequest request;

	request.set_a(10);
	request.set_b(20);

	std::cout << "Before serialization:" << std::endl;
	std::cout << "a = " << request.a() << std::endl;
	std::cout << "b = " << request.b() << std::endl;
	//2.将cpp对象序列化成字节数据
	std::string data;
	if(!request.SerializeToString(&data))
      {
         std::cerr << "Serialization failed!" << std::endl;
	 return 1;

      }
	std::cout << "Serialized size: "
		  << data.size()
		  << " bytes"
		  <<std::endl;
	//3.模拟服务器收到数据
	minirpc::AddRequest received_request;
	//4.将字节数据反序列化回cpp对象
	if(!received_request.ParseFromString(data))
        {
		std::cerr << "Deserialization failed" << std::endl;
		return 1;
        }
	std::cout << "After deserialization:" << std::endl;
	std::cout << "a = " << received_request.a() <<std::endl;
	std::cout << "b = " << received_request.b() <<std::endl;

	return 0;
}
