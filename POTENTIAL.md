# Pipecat OpenTelemetry Audit: Potential Gaps and Alignment Actions

This document details the audit of Pipecat's tracing implementation against the latest OpenTelemetry Generative AI Semantic Conventions. It is structured file-by-file according to the OTel specifications under `docs/gen-ai/`.

---

## gen-ai-spans.md (Common GenAI Spans Spec)

### Audit Findings & Gaps

1. **Span Kind & Naming Convention Mismatch**
   - **OTel Spec:** Client operations (like chat or generate content) should have span kind `CLIENT` and follow the naming pattern `{gen_ai.operation.name} {gen_ai.request.model}`.
   - **Pipecat Current:** Spans are created as internal/default spans (`tracer.start_span(...)` or `tracer.start_as_current_span(...)`) instead of `CLIENT`. Span names are generic internal strings like `"llm"`, `"stt"`, `"tts"`, `"conversation"`, `"turn"`, and `"{operation}"` (where operation is `llm_setup`, `llm_tool_call`, etc.).

2. **Custom vs. Standard Attributes**
   - **OTel Spec:** `gen_ai.request.stream` (boolean)
     - **Pipecat Current:** Uses custom attribute `stream` (missing `gen_ai.request.` prefix).
   - **OTel Spec:** `gen_ai.response.id` (string)
     - **Pipecat Current:** Uses custom attribute `response.id` (missing `gen_ai.` prefix).
   - **OTel Spec:** `gen_ai.conversation.id` (string)
     - **Pipecat Current:** Uses custom attribute `conversation.id` (missing `gen_ai.` prefix).
   - **OTel Spec:** `gen_ai.response.time_to_first_chunk` (double, in seconds)
     - **Pipecat Current:** Uses custom attribute `metrics.ttfb` (missing `gen_ai.` prefix, and may differ in unit/concept).
   - **OTel Spec:** `gen_ai.tool.definitions` (array of objects/JSON)
     - **Pipecat Current:** Uses custom attribute `tools.definitions` (missing `gen_ai.` prefix).

3. **Missing Standard Attributes**
   - **OTel Spec:** `gen_ai.response.model` (string) - Name of the model that generated the response (e.g. `gpt-4-0613` vs requested `gpt-4`).
     - **Pipecat Current:** Only sets `gen_ai.request.model`. Does not populate response model.
   - **OTel Spec:** `gen_ai.response.finish_reasons` (string[]) - Array of reasons the model stopped generating tokens.
     - **Pipecat Current:** Not tracked or recorded on spans.
   - **OTel Spec:** `gen_ai.request.stop_sequences` (string[]) - Stop sequences provided to the model.
     - **Pipecat Current:** Not recorded.
   - **OTel Spec:** `gen_ai.conversation.compacted` (boolean) - Context compaction flag.
     - **Pipecat Current:** Not recorded.
   - **OTel Spec:** `gen_ai.request.reasoning.level` (string) - Effort level for reasoning models.
     - **Pipecat Current:** Not recorded.
   - **OTel Spec:** `gen_ai.request.choice.count` (int) - Number of choices requested.
     - **Pipecat Current:** Not recorded.

4. **Token Usage Attributes Mismatch**
   - **OTel Spec:** `gen_ai.usage.reasoning.output_tokens` (with dot separating `reasoning.output_tokens`).
     - **Pipecat Current:** Uses custom attribute `gen_ai.usage.reasoning_tokens` (with underscore).

5. **Prompt and Completion Message Logging (Opt-In)**
   - **OTel Spec:** Recommends logging chat history and outputs via `gen_ai.input.messages` and `gen_ai.output.messages` adhering to structured JSON schemas.
     - **Pipecat Current:** Logs raw input/output via custom strings/JSON `input` and `output` which do not conform to the schema or naming convention.

---

## gen-ai-metrics.md (GenAI Metrics Spec)

### Audit Findings & Gaps

1. **Lack of Standard OTel Metrics Instrumentation**
   - **OTel Spec:** Standard GenAI clients should publish metrics:
     - `gen_ai.client.operation.duration` (Histogram, unit `s`): Measures duration of client operations.
     - `gen_ai.client.token.usage` (Counter, unit `1`): Measures token usage, using the `gen_ai.token.type` attribute (`input` or `output`).
   - **Pipecat Current:** Pipecat uses internal `MetricsFrame` which propagates metrics downstream, but it does not register or emit standard OTel Histograms/Counters for operations or token usage. All metrics are currently attached solely as attributes on the spans.

---

## gen-ai-events.md (GenAI Events Spec)

### Audit Findings & Gaps

1. **No OTel Events Emitted**
   - **OTel Spec:** Operations should emit structured OTel events representing message exchanges and tool calls:
     - `gen_ai.choice`
     - `gen_ai.user.message`
     - `gen_ai.assistant.message`
     - `gen_ai.system.message`
     - `gen_ai.tool.message`
     - `gen_ai.tool.call`
     - `gen_ai.tool.call.result`
   - **Pipecat Current:** Pipecat doesn't implement Span Events or Log Record Events for prompt/completion messages. It only uses span-level attributes.

---

## gen-ai-agent-spans.md (GenAI Agent Spans Spec)

### Audit Findings & Gaps

1. **Tool Execution Naming & Kind Mismatch**
   - **OTel Spec:** Tool execution spans should have span name `execute_tool {gen_ai.tool.name}` and span kind `CLIENT`.
   - **Pipecat Current:** Traced as internal/default spans under names like `llm_tool_call` and `llm_tool_result`.
2. **Tool Call Attributes Mismatch**
   - **OTel Spec:** Tool call attributes are defined as:
     - `gen_ai.tool.name` (string)
     - `gen_ai.tool.call.id` (string)
     - `gen_ai.tool.call.arguments` (any/JSON)
     - `gen_ai.tool.call.result` (any/JSON)
   - **Pipecat Current:** Uses custom attributes:
     - `tool.function_name`
     - `tool.call_id`
     - `tool.arguments`
     - `tool.result`
     - `tool.result_status`
3. **No Invoke Agent or Plan Spans**
   - **OTel Spec:** Suggests creating high-level spans for `invoke_agent` and `plan` operations.
   - **Pipecat Current:** Standard agent execution and planning phases are not represented via standardized spans.

---

## openai.md (OpenAI Specific Conventions Spec)

### Audit Findings & Gaps

1. **Missing OpenAI-Specific Attributes**
   - **OTel Spec:** Recommends setting:
     - `openai.request.service_tier` (string)
     - `openai.response.service_tier` (string)
     - `openai.response.system_fingerprint` (string)
     - `openai.api.type` (string: `completions`, `chat_completions`, `assistants`, etc.)
   - **Pipecat Current:** None of these attributes are recorded on OpenAI-related spans (e.g. `traced_openai_realtime`). Pipecat records non-standard custom attributes like `session.turn_detection.enabled` instead.

---

## anthropic.md (Anthropic Specific Conventions Spec)

### Audit Findings & Gaps

1. **Missing Reasoning Effort Level**
   - **OTel Spec:** Recommends recording `gen_ai.request.reasoning.level` mapping to `output_config.effort` parameter.
   - **Pipecat Current:** Not implemented in `traced_llm` or Anthropic services.

---

## aws-bedrock.md (AWS Bedrock Specific Conventions Spec)

### Audit Findings & Gaps

1. **No AWS Bedrock Specific Conventions Recorded**
   - **OTel Spec:** Specifies attributes like `aws.bedrock.guardrail.id` and `aws.bedrock.knowledge_base.id`.
   - **Pipecat Current:** Generic `traced_llm` is used. Bedrock-specific attributes are completely absent.

---

## azure-ai-inference.md (Azure AI Inference Spec)

### Audit Findings & Gaps

1. **No Azure AI Inference Specific Conventions Recorded**
   - **OTel Spec:** Requires `azure.resource_provider.namespace` set to `"Microsoft.CognitiveServices"`.
   - **Pipecat Current:** Not implemented.

---

## mcp.md (Model Context Protocol Spec)

### Audit Findings & Gaps

1. **No MCP Span or Attribute Tracing**
   - **OTel Spec:** Outlines detailed client/server conventions for MCP operations with client-side spans (`mcp.client`), server-side spans (`mcp.server`), methods (`mcp.method.name`), protocol versions (`mcp.protocol.version`), session IDs (`mcp.session.id`), and context propagation via `_meta` property bag (using `traceparent`, `tracestate`, `baggage`).
   - **Pipecat Current:** Pipecat does not trace MCP actions or propagate OTel tracing context over MCP endpoints.

---

## gen-ai-exceptions.md (GenAI Exceptions Spec)

### Audit Findings & Gaps

1. **Lack of Structured Exception Logging**
   - **OTel Spec:** Exceptions should be logged as `gen_ai.client.operation.exception` events with attributes `exception.message`, `exception.type`, and `exception.stacktrace`, logged at severity level `WARN` (severity number 13).
   - **Pipecat Current:** Exceptions are caught to set span status to `ERROR` or pushed via Pipecat's custom frame errors, but are not logged as standard OTel events or logs matching the spec.