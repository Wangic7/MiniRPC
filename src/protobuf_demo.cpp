#include <iostream>
#include <string>

#include "../proto/calculator.pb.h"

int main()
{
    minirpc::AddRequest request;

    request.set_a(10);
    request.set_b(20);

    std::cout << "Before serialization:" << std::endl;
    std::cout << "a = "
              << request.a()
              << std::endl;

    std::cout << "b = "
              << request.b()
              << std::endl;


    std::string data;


    if(request.SerializeToString(&data))
    {
        std::cout << "Serialize success!" << std::endl;

        std::cout << "Data size: "
                  << data.size()
                  << std::endl;
    }


    minirpc::AddRequest new_request;


    if(new_request.ParseFromString(data))
    {
        std::cout << "Deserialize success!"
                  << std::endl;

        std::cout << "a = "
                  << new_request.a()
                  << std::endl;

        std::cout << "b = "
                  << new_request.b()
                  << std::endl;
    }


    return 0;
}



