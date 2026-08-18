package playground

import _ "embed" // Enable the go:embed directives below.

type exampleScript struct {
	filename string
	source   string
}

//go:embed examples/linear.go.txt
var linearSource string

//go:embed examples/branch_join.go.txt
var branchJoinSource string

//go:embed examples/retry.go.txt
var retrySource string

//go:embed examples/large_pipeline.go.txt
var largePipelineSource string

//go:embed examples/http_request.go.txt
var httpRequestSource string

//go:embed examples/permanent_failure.go.txt
var permanentFailureSource string

var exampleScripts = []exampleScript{
	{filename: "linear.go", source: linearSource},
	{filename: "branch_join.go", source: branchJoinSource},
	{filename: "retry.go", source: retrySource},
	{filename: "large_pipeline.go", source: largePipelineSource},
	{filename: "http_request.go", source: httpRequestSource},
	{filename: "permanent_failure.go", source: permanentFailureSource},
}

func exampleSource(filename string) (string, bool) {
	for _, script := range exampleScripts {
		if script.filename == filename {
			return script.source, true
		}
	}

	return "", false
}
