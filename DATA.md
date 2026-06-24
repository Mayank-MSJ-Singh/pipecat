# Pipecat OpenTelemetry Audit Data

## Files Processed

* `src/pipecat/utils/tracing/setup.py`
* `src/pipecat/utils/tracing/tracing_context.py`
* `src/pipecat/utils/tracing/service_attributes.py`
* `src/pipecat/utils/tracing/service_decorators.py`
* `src/pipecat/utils/tracing/turn_trace_observer.py`

## Spans

* `src/pipecat/utils/tracing/service_decorators.py`:
  * **Span Name:** `"tts"`
    * **Function:** Patched `create_audio_context` inside `traced_tts`
    * **Kind:** Default/Internal (started via `tracer.start_span("tts", context=parent)`)
  * **Span Name:** `"stt"`
    * **Function:** Patched `push_frame` (triggered on `TranscriptionFrame` or `VADUserStartedSpeakingFrame` pre-push) inside `traced_stt`
    * **Kind:** Default/Internal (started via `tracer.start_span("stt", context=parent, start_time=...)`)
  * **Span Name:** `"llm"`
    * **Function:** `wrapper` inside `traced_llm`
    * **Kind:** Default/Internal (started via `tracer.start_as_current_span("llm", context=parent)`)
  * **Span Name:** `"{operation}"` (where operation can be `"llm_setup"`, `"llm_tool_call"`, `"llm_tool_result"`, or `"llm_response"`)
    * **Function:** `wrapper` inside `traced_gemini_live`
    * **Kind:** Default/Internal (started via `tracer.start_as_current_span(f"{operation}", context=parent)`)
  * **Span Name:** `"{operation}"` (where operation can be `"llm_setup"`, `"llm_request"`, or `"llm_response"`)
    * **Function:** `wrapper` inside `traced_openai_realtime`
    * **Kind:** Default/Internal (started via `tracer.start_as_current_span(f"{operation}", context=parent)`)
* `src/pipecat/utils/tracing/turn_trace_observer.py`:
  * **Span Name:** `"conversation"`
    * **Function:** `start_conversation_tracing` (called on `StartFrame` processing or when turn 1 starts)
    * **Kind:** Default/Internal (started via `self._tracer.start_span("conversation")`)
  * **Span Name:** `"turn"`
    * **Function:** `_handle_turn_started` (triggered via `on_turn_started` event of the turn tracker)
    * **Kind:** Default/Internal (started via `self._tracer.start_span("turn", context=parent_context)`)

## Attributes

Every set_attribute call and its source in the processed files:
* `gen_ai.provider.name` (in `add_tts_span_attributes`, `add_stt_span_attributes`, `add_llm_span_attributes`, `add_gemini_live_span_attributes`, `add_openai_realtime_span_attributes`)
* `gen_ai.request.model` (in `add_tts_span_attributes`, `add_stt_span_attributes`, `add_llm_span_attributes`, `add_gemini_live_span_attributes`, `add_openai_realtime_span_attributes`)
* `gen_ai.operation.name` (in `add_tts_span_attributes`, `add_stt_span_attributes`, `add_llm_span_attributes`, `add_gemini_live_span_attributes`, `add_openai_realtime_span_attributes`)
* `gen_ai.output.type` (in `add_tts_span_attributes`, `add_llm_span_attributes`)
* `voice_id` (in `add_tts_span_attributes`, `add_gemini_live_span_attributes`)
* `text` (in `add_tts_span_attributes`, `service_decorators.py:attach_run_tts_attributes`)
* `metrics.character_count` (in `add_tts_span_attributes`, `service_decorators.py:attach_run_tts_attributes`)
* `metrics.ttfb` (in `add_tts_span_attributes`, `add_stt_span_attributes`, `add_llm_span_attributes`, `service_decorators.py:traced_push_frame`, `service_decorators.py:handle_post_push`, `service_decorators.py:wrapper` in `traced_llm`/`traced_gemini_live`/`traced_openai_realtime`)
* `settings.{key}` (in `add_tts_span_attributes`, `add_stt_span_attributes`, `add_gemini_live_span_attributes`)
* `vad_enabled` (in `add_stt_span_attributes`)
* `transcript` (in `add_stt_span_attributes`, `add_gemini_live_span_attributes`, `add_openai_realtime_span_attributes`, `service_decorators.py:handle_post_push`)
* `is_final` (in `add_stt_span_attributes`, `service_decorators.py:handle_post_push`)
* `language` (in `add_stt_span_attributes`, `add_gemini_live_span_attributes`, `service_decorators.py:handle_post_push`)
* `user_id` (in `add_stt_span_attributes`, `service_decorators.py:handle_post_push`)
* `stream` (in `add_llm_span_attributes`)
* `input` (in `add_llm_span_attributes`, `add_openai_realtime_span_attributes`)
* `output` (in `add_llm_span_attributes`, `service_decorators.py:wrapper` in `traced_llm`)
* `tools` (in `add_llm_span_attributes`)
* `tool_count` (in `add_llm_span_attributes`)
* `tool_choice` (in `add_llm_span_attributes`)
* `gen_ai.system_instructions` (in `add_llm_span_attributes`)
* `gen_ai.request.temperature`, `gen_ai.request.max_tokens`, `gen_ai.request.max_completion_tokens`, `gen_ai.request.top_p`, `gen_ai.request.top_k`, `gen_ai.request.frequency_penalty`, `gen_ai.request.presence_penalty`, `gen_ai.request.seed` (in `add_llm_span_attributes`)
* `param.{key}` (in `add_llm_span_attributes`)
* `extra.{key}` (in `add_llm_span_attributes`)
* `service.operation` (in `add_gemini_live_span_attributes`, `add_openai_realtime_span_attributes`)
* `modalities` (in `add_gemini_live_span_attributes`)
* `transcript.is_input` (in `add_gemini_live_span_attributes`, `add_openai_realtime_span_attributes`)
* `text_output` (in `add_gemini_live_span_attributes`)
* `audio.data_size_bytes` (in `add_gemini_live_span_attributes`, `add_openai_realtime_span_attributes`)
* `tools.count` (in `add_gemini_live_span_attributes`, `add_openai_realtime_span_attributes`)
* `tools.available` (in `add_gemini_live_span_attributes`, `add_openai_realtime_span_attributes`)
* `tools.names` (in `add_gemini_live_span_attributes`, `add_openai_realtime_span_attributes`)
* `tools.definitions` (in `add_gemini_live_span_attributes`, `add_openai_realtime_span_attributes`)
* `settings.vad.disabled`, `settings.vad.start_sensitivity`, `settings.vad.end_sensitivity` (in `add_gemini_live_span_attributes`)
* `function_calls.count`, `function_calls.first_name` (in `add_openai_realtime_span_attributes`)
* `session.{key}`, `session.turn_detection.enabled`, `session.turn_detection.{td_key}` (in `add_openai_realtime_span_attributes`)
* `tts.interrupted` (in `service_decorators.py:end_tts_span`)
* `stt.incomplete` (in `service_decorators.py:handle_post_push`)
* `gen_ai.usage.input_tokens` (in `service_decorators.py:_add_token_usage_to_span`)
* `gen_ai.usage.output_tokens` (in `service_decorators.py:_add_token_usage_to_span`)
* `gen_ai.usage.cache_read.input_tokens` (in `service_decorators.py:_add_token_usage_to_span`)
* `gen_ai.usage.cache_creation.input_tokens` (in `service_decorators.py:_add_token_usage_to_span`)
* `gen_ai.usage.reasoning_tokens` (in `service_decorators.py:_add_token_usage_to_span`)
* `context_system_instruction` (in `service_decorators.py:traced_gemini_live` - `llm_setup` operation)
* `tool.function_name` (in `service_decorators.py:traced_gemini_live` - `llm_tool_call` & `llm_tool_result` operations)
* `tool.call_id` (in `service_decorators.py:traced_gemini_live` - `llm_tool_call` & `llm_tool_result` operations)
* `tool.calls_count` (in `service_decorators.py:traced_gemini_live` - `llm_tool_call` operation)
* `tool.all_function_names` (in `service_decorators.py:traced_gemini_live` - `llm_tool_call` operation)
* `tool.arguments` (in `service_decorators.py:traced_gemini_live` - `llm_tool_call` operation)
* `tool.result` (in `service_decorators.py:traced_gemini_live` - `llm_tool_result` operation)
* `tool.result_status` (in `service_decorators.py:traced_gemini_live` - `llm_tool_result` operation)
* `tokens.prompt` (in `service_decorators.py:traced_gemini_live` / `traced_openai_realtime` - `llm_response` operation)
* `tokens.completion` (in `service_decorators.py:traced_gemini_live` / `traced_openai_realtime` - `llm_response` operation)
* `tokens.total` (in `service_decorators.py:traced_gemini_live` / `traced_openai_realtime` - `llm_response` operation)
* `output_modality` (in `service_decorators.py:traced_gemini_live` - `llm_response` operation)
* `turn_complete` (in `service_decorators.py:traced_gemini_live` - `llm_response` operation)
* `session_properties` (in `service_decorators.py:traced_openai_realtime` - `llm_setup` operation)
* `instructions` (in `service_decorators.py:traced_openai_realtime` - `llm_setup` operation)
* `context_messages` (in `service_decorators.py:traced_openai_realtime` - `llm_request` operation)
* `response.status` (in `service_decorators.py:traced_openai_realtime` - `llm_response` operation)
* `response.id` (in `service_decorators.py:traced_openai_realtime` - `llm_response` operation)
* `response.output_items` (in `service_decorators.py:traced_openai_realtime` - `llm_response` operation)
* `function_calls` (in `service_decorators.py:traced_openai_realtime` - `llm_response` operation)
* `function_calls.all_names` (in `service_decorators.py:traced_openai_realtime` - `llm_response` operation)
* `turn.user_bot_latency_seconds` (in `turn_trace_observer.py:_handle_latency_measured`)
* `conversation.id` (in `turn_trace_observer.py:start_conversation_tracing`, `_handle_turn_started`)
* `conversation.type` (in `turn_trace_observer.py:start_conversation_tracing`)
* `turn.was_interrupted` (in `turn_trace_observer.py:end_conversation_tracing`, `_handle_turn_ended`)
* `turn.ended_by_conversation_end` (in `turn_trace_observer.py:end_conversation_tracing`)
* `turn.number` (in `turn_trace_observer.py:_handle_turn_started`)
* `turn.type` (in `turn_trace_observer.py:_handle_turn_started`)
* `turn.duration_seconds` (in `turn_trace_observer.py:_handle_turn_ended`)

## GenAI Attributes

* `gen_ai.provider.name`
* `gen_ai.request.model`
* `gen_ai.operation.name`
* `gen_ai.output.type`
* `gen_ai.system_instructions`
* `gen_ai.request.temperature`
* `gen_ai.request.max_tokens`
* `gen_ai.request.max_completion_tokens`
* `gen_ai.request.top_p`
* `gen_ai.request.top_k`
* `gen_ai.request.frequency_penalty`
* `gen_ai.request.presence_penalty`
* `gen_ai.request.seed`
* `gen_ai.usage.input_tokens`
* `gen_ai.usage.output_tokens`
* `gen_ai.usage.cache_read.input_tokens`
* `gen_ai.usage.cache_creation.input_tokens`
* `gen_ai.usage.reasoning_tokens`

## Custom Attributes

* `voice_id`
* `text`
* `metrics.character_count`
* `metrics.ttfb`
* `settings.{key}` (where settings is from `ServiceSettings`)
* `vad_enabled`
* `transcript`
* `is_final`
* `language`
* `user_id`
* `stream`
* `input` (JSON-serialized input/context messages)
* `output` (aggregated output response text)
* `tools` (JSON-serialized tools configuration)
* `tool_count`
* `tool_choice`
* `param.{key}` (other LLM parameters)
* `extra.{key}` (extra parameters)
* `service.operation`
* `modalities`
* `transcript.is_input`
* `text_output`
* `audio.data_size_bytes`
* `tools.count`
* `tools.available`
* `tools.names`
* `tools.definitions`
* `settings.vad.disabled`
* `settings.vad.start_sensitivity`
* `settings.vad.end_sensitivity`
* `function_calls.count`
* `function_calls.first_name`
* `session.{key}`
* `session.turn_detection.enabled`
* `session.turn_detection.{td_key}`
* `tts.interrupted`
* `stt.incomplete`
* `context_system_instruction`
* `tool.function_name`
* `tool.call_id`
* `tool.calls_count`
* `tool.all_function_names`
* `tool.arguments`
* `tool.result`
* `tool.result_status`
* `tokens.prompt`
* `tokens.completion`
* `tokens.total`
* `output_modality`
* `turn_complete`
* `session_properties`
* `instructions`
* `context_messages`
* `response.status`
* `response.id`
* `response.output_items`
* `function_calls`
* `function_calls.all_names`
* `turn.user_bot_latency_seconds`
* `conversation.id`
* `conversation.type`
* `turn.was_interrupted`
* `turn.ended_by_conversation_end`
* `turn.number`
* `turn.type`
* `turn.duration_seconds`

## Tool Tracing

* Attributes defined in `src/pipecat/utils/tracing/service_attributes.py`:
  * `tools` (JSON-serialized configuration for LLMs)
  * `tool_count` (available tools count)
  * `tool_choice` (tool choice configuration)
  * `tools.count` (Gemini Live and OpenAI Realtime tools count)
  * `tools.available` (boolean flag for tool availability)
  * `tools.names` (comma-separated list of tool names)
  * `tools.definitions` (JSON-serialized tool definitions)
  * `function_calls.count` (count of function calls in OpenAI Realtime)
  * `function_calls.first_name` (name of the first function call in OpenAI Realtime)
* Functionality defined in `src/pipecat/utils/tracing/service_decorators.py`:
  * `traced_gemini_live` (`llm_setup` operation): Serializes and records tool schemas/definitions to `tools_serialized` and `tools` lists.
  * `traced_gemini_live` (`llm_tool_call` operation): Extracts first function name (`tool.function_name`), call ID (`tool.call_id`), total count of calls (`tool.calls_count`), all function names being called (`tool.all_function_names`), and JSON-serialized arguments (`tool.arguments`).
  * `traced_gemini_live` (`llm_tool_result` operation): Extracts function name, call ID, JSON-serialized tool output (`tool.result`), and success/error status (`tool.result_status`).
  * `traced_openai_realtime` (`llm_setup` operation): Extracts tools and serialized tools from `session_properties` or `_context`.
  * `traced_openai_realtime` (`llm_response` operation): Extracts assistant function calls (`function_calls`), count (`function_calls.count`), and comma-separated function names (`function_calls.all_names`).

## STT Tracing

* Attributes defined in `src/pipecat/utils/tracing/service_attributes.py`:
  * `vad_enabled`
  * `transcript`
  * `is_final`
  * `language`
  * `user_id`
  * `metrics.ttfb` (Time to first byte)
  * `settings.{key}` (STT specific configuration settings)
* Functionality defined in `src/pipecat/utils/tracing/service_decorators.py` (`traced_stt` decorator):
  * Lays out a descriptor that wraps `push_frame` and `stop_ttfb_metrics` on the STT service class.
  * Scopes the `"stt"` span from the start of user speech (anchored using `VADUserStartedSpeakingFrame` timestamp) to the finalized transcript frame (`finalized=True`).
  * Updates interim transcription segments defensively to handle sequential segments instead of losing them.
  * Handles the turn completion fallback: if the user stops speaking before a finalized transcript is completed (e.g. timeout), it force-stops the TTFB metrics, closes the span, and marks it with `stt.incomplete = True`.

## TTS Tracing

* Attributes defined in `src/pipecat/utils/tracing/service_attributes.py`:
  * `voice_id`
  * `text` (the text being synthesized)
  * `metrics.character_count`
  * `metrics.ttfb` (Time to first byte)
  * `settings.{key}` (TTS specific configuration settings)
* Functionality defined in `src/pipecat/utils/tracing/service_decorators.py` (`traced_tts` decorator):
  * Wires up tracing dynamically on class definition by wrapping `setup()`.
  * Installs patches on audio context methods (`create_audio_context`, `append_to_audio_context`, `push_frame`, `remove_audio_context`, `on_audio_context_completed`, `reset_active_audio_context`) to manage the span lifecycle.
  * The `"tts"` span starts in `create_audio_context` and ends in `append_to_audio_context` on `TTSStoppedFrame`, or on completion / removal.
  * Records `metrics.ttfb` when a `MetricsFrame` with `TTFBMetricsData` is pushed, correlating it with the active context.
  * Attaches synthesized text and character counts to the in-flight TTS span on `run_tts` invocation.
  * If the audio context is reset/interrupted, the span is closed and marked `tts.interrupted = True`.

## VAD Tracing

* Attributes defined in `src/pipecat/utils/tracing/service_attributes.py`:
  * `vad_enabled` (STT VAD setting)
  * `settings.vad.disabled` (Gemini Live VAD setting)
  * `settings.vad.start_sensitivity`, `settings.vad.end_sensitivity` (Gemini Live VAD sensitivity settings)
  * `session.turn_detection.enabled`, `session.turn_detection.{td_key}` (OpenAI Realtime VAD/turn detection settings)
* Functionality defined in `src/pipecat/utils/tracing/turn_trace_observer.py` / `service_decorators.py`:
  * `VADUserStartedSpeakingFrame` triggers the start anchor of `"stt"` spans.
  * `TurnTraceObserver` uses event handlers on `TurnTrackingObserver` (`on_turn_started` / `on_turn_ended`) which is driven by VAD user start/stop strategies.

## Conversation / Turn Tracing

* Attributes defined in `src/pipecat/utils/tracing/service_attributes.py`:
  * `transcript.is_input` (Boolean indicator if transcript is user input/bot output in Gemini Live / OpenAI Realtime)
  * `src/pipecat/utils/tracing/tracing_context.py`:
    * Class `TracingContext`: Pipeline-scoped tracing context.
    * `set_conversation_context(span_context, conversation_id)`: Stores conversation ID and sets the conversation OpenTelemetry context (via `NonRecordingSpan` and `set_span_in_context`).
    * `get_conversation_context()`: Returns OpenTelemetry context for the current conversation.
    * `set_turn_context(span_context)`: Sets the current turn context.
    * `get_turn_context()`: Returns OpenTelemetry context for the current turn.
    * `conversation_id`: Property to retrieve the current conversation ID.
    * `generate_conversation_id()`: Generates a random UUID4 string for conversation identification.
  * `src/pipecat/utils/tracing/service_decorators.py` helpers:
    * `_get_turn_context(service)`: Retrieves the active turn span's OpenTelemetry context from `TracingContext`.
    * `_get_parent_service_context(service)`: Retrieves the conversation's span context or falls back to the current context if unavailable.
    * Decorators (`traced_tts`, `traced_stt`, `traced_llm`, `traced_gemini_live`, `traced_openai_realtime`) parent their created spans (`"tts"`, `"stt"`, `"llm"`, operation spans) to the active turn context or parent service context.
  * `src/pipecat/utils/tracing/turn_trace_observer.py` (`TurnTraceObserver` observer):
    * Starts a `"conversation"` span on receiving `StartFrame` or when turn 1 starts. Sets `conversation.id` and `conversation.type = "voice"`. Updates `TracingContext` conversation context.
    * Starts a `"turn"` span (child of `"conversation"` span context) when the `on_turn_started` event fires. Sets `turn.number`, `turn.type = "conversation"`, and `conversation.id`.
    * Ends the `"turn"` span when `on_turn_ended` fires. Attaches `turn.duration_seconds` and `turn.was_interrupted`.
    * Sets `turn.user_bot_latency_seconds` on the current turn span when `on_latency_measured` fires.
    * Closes active turn and conversation spans, updating `TracingContext` context accordingly.

## Notes / Potential Issues

* `src/pipecat/utils/tracing/setup.py`:
  * Sets resource attributes: `service.name`, `service.instance.id` (from `HOSTNAME` environment variable), and `deployment.environment` (from `ENVIRONMENT` environment variable).
  * Handles optional `opentelemetry` dependencies gracefully by catching `ImportError` on imports.
  * Allows registering custom exporters and console span exporters.
* `src/pipecat/utils/tracing/tracing_context.py`:
  * Created by `PipelineWorker`, passed downstream via `StartFrame` to `TurnTraceObserver` and various services.
  * Protects against missing `opentelemetry` dependencies with dynamic availability checks (`is_tracing_available()`).
* `src/pipecat/utils/tracing/service_decorators.py`:
  * The decorators perform complex metaclass-like descriptor magic at class definition time by intercepting method definitions (e.g. `__set_name__` in `_TracedTTSDescriptor` / `_TracedSTTDescriptor`).
  * Patches internal instance methods (`push_frame`, `create_audio_context`, etc.) dynamically on service initialization.
  * Avoids crashing on missing dependencies or disabled tracing via check flags (`_tracing_enabled` or `is_tracing_available()`).
* `src/pipecat/utils/tracing/turn_trace_observer.py`:
  * Leverages events from `TurnTrackingObserver` and `UserBotLatencyObserver` to create hierarchical context-driven spans.
  * Safe fallback check when `opentelemetry` is not installed or configured.
  * Generates conversation IDs dynamically via UUID4.
