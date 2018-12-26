[![](https://godoc.org/github.com/jackc/puddle?status.svg)](https://godoc.org/github.com/jackc/puddle)
[![Build Status](https://travis-ci.org/jackc/puddle.svg)](https://travis-ci.org/jackc/puddle)

# Puddle

Puddle is a tiny generic resource pool library for Go that uses the standard context library to signal cancellation of acquires. It is designed to contain the minimum functionality a resource pool needs that cannot be implemented without concerrency concerns. For example, a database connection pool may use puddle internally and implement health checks and keep-alive behavior without needing to implement any concurrent code of its own.

## Features

* Acquire cancellation via context standard library
* Statistics API for monitoring pool pressure
* No dependencies outside of standard library
* High performance
* 100% test coverage

## License

MIT
