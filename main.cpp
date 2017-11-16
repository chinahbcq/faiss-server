#include <stdlib.h>
#include <stdio.h>
#include <grpc++/grpc++.h>
#include "faiss_logic.h"
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

void RunServer()
{
	std::string server_address("0.0.0.0:3838");
	FaissServiceImpl service;
	ServerBuilder builder;
	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
	builder.RegisterService(&service);
	std::unique_ptr<Server> server(builder.BuildAndStart());
	std::cout << "Server start on " << server_address << std::endl;
	
	//persist thread
	std::thread th(FaissServiceImpl::PersistIndexPeriod, &service, 5);
	th.join();
	server->Wait();
}
int main()
{
	RunServer();
	return 0;
}
