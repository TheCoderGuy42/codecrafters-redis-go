package main

import "log"

func logging() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}
