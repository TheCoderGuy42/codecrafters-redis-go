package main

type redisServer struct {
	ram      *SafeMap
	dbConfig map[string]string
}

func (*redisServer) startServer(os_args []string) {

}
