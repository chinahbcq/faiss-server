#include <stdlib.h>
#include <stdio.h>
#include <grpc++/grpc++.h>
#include "faiss_logic.h"
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

DEFINE_string(host, "0.0.0.0", "the server host");
DEFINE_int32(port, 3838, "the server port");
DEFINE_int32(persist_time, 10, "persist time");
DEFINE_double(euclid_thresh, 30.0f, "euclid thresh hold");
DEFINE_int32(nprobes, 32, "number of probes");

GlobalConfig globalConfig;

void RunServer()
{
	globalConfig.Port = FLAGS_port;
	globalConfig.Host = FLAGS_host;
	globalConfig.PersistTime = FLAGS_persist_time;
	globalConfig.EuclidThresh = FLAGS_euclid_thresh;
	globalConfig.NProbes = FLAGS_nprobes;

	std::string srv = FLAGS_host + ":" + std::to_string(FLAGS_port);
	std::string server_address(srv);
	FaissServiceImpl service;
	ServerBuilder builder;
	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
	builder.RegisterService(&service);
	std::unique_ptr<Server> server(builder.BuildAndStart());
	LOG(INFO)<< "Server start on " << server_address << std::endl;
	
	//persist thread
	std::thread th(FaissServiceImpl::PersistIndexPeriod, &service, FLAGS_persist_time);
	th.join();
	server->Wait();
}

int main(int argc, char **argv)
{
	google::InitGoogleLogging(argv[0]);
	google::ParseCommandLineFlags(&argc, &argv, true);
	RunServer();
	return 0;
}