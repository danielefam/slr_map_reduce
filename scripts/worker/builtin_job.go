package main

import (
	"scripts/jobs/wordcount"
	"scripts/mrjob"
)

// loadInjectedJob is used by local tests and direct worker builds.
// The mapreduce orchestrator builds a generated worker package that replaces
// this binding with the job package selected through -job.
func loadInjectedJob() (mrjob.Mapper, mrjob.Reducer, error) {
	return wordcount.NewMapper(), wordcount.NewReducer(), nil
}
