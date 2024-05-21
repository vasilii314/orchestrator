## Go Orchestrator
### *Runs on Linux as its scheduler relies on /proc filesystem.*

## CLI

- `go run main.go worker` starts a worker at `localhost:5555` (run `go run main.go worker --help` for more info)
- `go run main.go manager -w 'localhost:5555'` starts a manager at `localhost:5554` listening to worker at `localhost:5555` (run `go run main.go manager --help` for more info)
- `go run main.go run --filename task1.json` starts a task defined in `task1.json` on a manager at `localhost:5554` (run `go run main.go run --help` for more info)
- `go run main.go status -m localhost:5554` lists all tasks manager at `localhost:5554` has (run `go run main.go status --help` for more info)
