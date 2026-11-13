package main

import (
	"encoding/json"
	"fmt"

	"taskforge/internal/job"
)

func main() {
	j := job.New("email", map[string]string{"to": "a@b.com"}, 2)
	out, _ := json.MarshalIndent(j, "", "  ")
	fmt.Println(string(out))
}