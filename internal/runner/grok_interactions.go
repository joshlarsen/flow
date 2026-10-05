package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

type grokQuestionOption struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}

type grokQuestion struct {
	Question    string               `json:"question"`
	Options     []grokQuestionOption `json:"options"`
	MultiSelect bool                 `json:"multiSelect"`
}

type grokAskUserQuestionRequest struct {
	SessionID  string         `json:"sessionId"`
	ToolCallID string         `json:"toolCallId"`
	Questions  []grokQuestion `json:"questions"`
	Mode       string         `json:"mode"`
}

type grokQuestionMapping struct {
	Question    string
	Field       string
	CustomField string
}

/** Converts Grok's native question extension into the bounded ACP form accepted by interaction providers. */
func normalizeGrokQuestions(params json.RawMessage) (json.RawMessage, []grokQuestionMapping, error) {
	var request grokAskUserQuestionRequest
	if err := json.Unmarshal(params, &request); err != nil {
		return nil, nil, fmt.Errorf("decode Grok user questions: %w", err)
	}
	if request.Mode != "" && request.Mode != "default" && request.Mode != "plan" {
		return nil, nil, fmt.Errorf("Grok user questions have invalid mode %q", request.Mode)
	}
	if len(request.Questions) < 1 || len(request.Questions) > maxUserInputQuestions {
		return nil, nil, fmt.Errorf("Grok user questions must contain 1 to %d questions", maxUserInputQuestions)
	}

	properties := make(map[string]any, len(request.Questions)*2)
	mappings := make([]grokQuestionMapping, 0, len(request.Questions))
	seenQuestions := map[string]bool{}
	for index, question := range request.Questions {
		questionText := strings.TrimSpace(question.Question)
		if questionText == "" || len(questionText) > 1000 || seenQuestions[questionText] {
			return nil, nil, fmt.Errorf("Grok question %d has invalid or duplicate text", index+1)
		}
		seenQuestions[questionText] = true
		if len(question.Options) < 2 || len(question.Options) > 100 {
			return nil, nil, fmt.Errorf("Grok question %d must contain 2 to 100 options", index+1)
		}

		field := fmt.Sprintf("question_%d", index)
		customField := field + "_custom"
		options := make([]map[string]any, 0, len(question.Options))
		seenOptions := map[string]bool{}
		for optionIndex, option := range question.Options {
			label := strings.TrimSpace(option.Label)
			if label == "" || len(label) > 75 || len(option.Description) > 1000 || seenOptions[label] {
				return nil, nil, fmt.Errorf("Grok question %d option %d is invalid or duplicated", index+1, optionIndex+1)
			}
			seenOptions[label] = true
			choice := map[string]any{"const": label, "title": label}
			if option.Description != "" {
				choice["description"] = option.Description
			}
			options = append(options, choice)
		}

		property := map[string]any{"title": fmt.Sprintf("Question %d", index+1)}
		if len(request.Questions) > 1 {
			property["description"] = questionText
		}
		if question.MultiSelect {
			property["type"] = "array"
			property["items"] = map[string]any{"type": "string", "anyOf": options}
			property["maxItems"] = len(options)
		} else {
			property["type"] = "string"
			property["oneOf"] = options
		}
		properties[field] = property
		properties[customField] = map[string]any{
			"type":        "string",
			"title":       "Other",
			"description": "Type your own answer instead of choosing an option above (optional).",
			"maxLength":   3000,
			"_meta": map[string]any{
				"_askUserQuestionCustomAnswer": map[string]any{"questionId": field, "isCustomAnswer": true},
			},
		}
		mappings = append(mappings, grokQuestionMapping{Question: questionText, Field: field, CustomField: customField})
	}

	message := "Please answer the following questions."
	if len(request.Questions) == 1 {
		message = strings.TrimSpace(request.Questions[0].Question)
	}
	form := map[string]any{
		"mode": "form", "message": message,
		"requestedSchema": map[string]any{"type": "object", "title": "Agent input", "properties": properties},
	}
	if request.SessionID != "" {
		form["sessionId"] = request.SessionID
	}
	if request.ToolCallID != "" {
		form["toolCallId"] = request.ToolCallID
	}
	payload, err := json.Marshal(form)
	return payload, mappings, err
}

/** Maps a provider-neutral decision back to Grok's native question response. */
func grokQuestionResponse(decision InteractionDecision, mappings []grokQuestionMapping) map[string]any {
	switch decision.Action {
	case InteractionAccept:
		answers := map[string][]string{}
		annotations := map[string]map[string]string{}
		for _, mapping := range mappings {
			if custom, ok := decision.Content[mapping.CustomField].(string); ok && strings.TrimSpace(custom) != "" {
				answers[mapping.Question] = []string{"Other"}
				annotations[mapping.Question] = map[string]string{"notes": strings.TrimSpace(custom)}
				continue
			}
			switch value := decision.Content[mapping.Field].(type) {
			case string:
				if value != "" {
					answers[mapping.Question] = []string{value}
				}
			case []string:
				answers[mapping.Question] = value
			case []any:
				for _, item := range value {
					if text, ok := item.(string); ok {
						answers[mapping.Question] = append(answers[mapping.Question], text)
					}
				}
			}
		}
		response := map[string]any{"outcome": "accepted", "answers": answers}
		if len(annotations) > 0 {
			response["annotations"] = annotations
		}
		return response
	case InteractionDecline:
		return map[string]any{"outcome": "skip_interview", "partial_answers": map[string][]string{}}
	default:
		return map[string]any{"outcome": "cancelled"}
	}
}
