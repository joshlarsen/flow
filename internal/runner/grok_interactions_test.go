package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

type recordingInteractionBroker struct {
	decision InteractionDecision
	requests []InteractionRequest
}

func (broker *recordingInteractionBroker) Resolve(_ context.Context, request InteractionRequest) (InteractionDecision, error) {
	broker.requests = append(broker.requests, request)
	return broker.decision, nil
}

func TestGrokQuestionsUseCanonicalElicitationBroker(t *testing.T) {
	config := testConfig()
	config.Workflow.Steps[0].AllowUserInput = true
	runner := NewRunner(config, "job-1", config.Workflow.Steps[0], &bytes.Buffer{})
	broker := &recordingInteractionBroker{decision: InteractionDecision{Action: InteractionAccept, Content: map[string]any{
		"question_0": "Production",
		"question_1": []any{"Tests", "Deploy"},
	}}}
	runner.broker = broker
	client := &runnerACPClient{runner: runner, state: &acpState{}, broker: broker}
	params := json.RawMessage(`{
		"sessionId":"session-1","toolCallId":"tool-1","mode":"default","questions":[
			{"question":"Which environment?","options":[{"label":"Staging","description":"Safe"},{"label":"Production","description":"Live"}]},
			{"question":"Which actions?","multiSelect":true,"options":[{"label":"Tests","description":"Run tests"},{"label":"Deploy","description":"Deploy it"}]}
		]
	}`)
	response, err := client.HandleExtensionMethod(context.Background(), "x.ai/ask_user_question", params)
	if err != nil {
		t.Fatal(err)
	}
	result := response.(map[string]any)
	answers := result["answers"].(map[string][]string)
	if result["outcome"] != "accepted" || strings.Join(answers["Which environment?"], ",") != "Production" || strings.Join(answers["Which actions?"], ",") != "Tests,Deploy" {
		t.Fatalf("unexpected Grok response: %#v", response)
	}
	if len(broker.requests) != 1 || broker.requests[0].Kind != InteractionElicitation {
		t.Fatalf("Grok question did not use the elicitation broker: %#v", broker.requests)
	}
	var form struct {
		Mode            string `json:"mode"`
		RequestedSchema struct {
			Properties map[string]map[string]any `json:"properties"`
		} `json:"requestedSchema"`
	}
	if err := json.Unmarshal(broker.requests[0].Payload, &form); err != nil {
		t.Fatal(err)
	}
	if form.Mode != "form" || len(form.RequestedSchema.Properties) != 4 {
		t.Fatalf("unexpected normalized form: %s", broker.requests[0].Payload)
	}
	choices := form.RequestedSchema.Properties["question_0"]["oneOf"].([]any)
	if choices[0].(map[string]any)["description"] != "Safe" {
		t.Fatalf("option descriptions were not preserved: %s", broker.requests[0].Payload)
	}
}

func TestGrokCustomAnswerAndDeclineMapping(t *testing.T) {
	mappings := []grokQuestionMapping{{Question: "How should it work?", Field: "question_0", CustomField: "question_0_custom"}}
	response := grokQuestionResponse(InteractionDecision{Action: InteractionAccept, Content: map[string]any{"question_0": "Default", "question_0_custom": "Use canaries"}}, mappings)
	answers := response["answers"].(map[string][]string)
	annotations := response["annotations"].(map[string]map[string]string)
	if strings.Join(answers["How should it work?"], ",") != "Other" || annotations["How should it work?"]["notes"] != "Use canaries" {
		t.Fatalf("unexpected custom answer response: %#v", response)
	}
	declined := grokQuestionResponse(InteractionDecision{Action: InteractionDecline}, mappings)
	if declined["outcome"] != "skip_interview" {
		t.Fatalf("unexpected decline response: %#v", declined)
	}
}

func TestGrokQuestionsRejectMoreThanFiveQuestions(t *testing.T) {
	questions := make([]map[string]any, maxUserInputQuestions+1)
	for index := range questions {
		questions[index] = map[string]any{"question": "Question " + string(rune('A'+index)), "options": []map[string]string{{"label": "Yes"}, {"label": "No"}}}
	}
	payload, _ := json.Marshal(map[string]any{"mode": "default", "questions": questions})
	if _, _, err := normalizeGrokQuestions(payload); err == nil || !strings.Contains(err.Error(), "1 to 5") {
		t.Fatalf("expected question limit error, got %v", err)
	}
}
