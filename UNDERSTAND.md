# 🧠 Pipecat: The Complete Deep-Dive Guide

> **Purpose**: This document is a comprehensive, educational manual for the [Pipecat](https://github.com/pipecat-ai/pipecat) framework. Every concept links directly to source code so you can click through and read the actual implementation. By the end, you'll understand how Pipecat works from the ground up.

---

## Table of Contents

1. [What is Pipecat?](#1-what-is-pipecat)
2. [Project Structure](#2-project-structure)
3. [The Foundation: BaseObject](#3-the-foundation-baseobject)
4. [Frames: The Data Currency](#4-frames-the-data-currency)
5. [FrameProcessor: The Processing Engine](#5-frameprocessor-the-processing-engine)
6. [Pipeline: Chaining Processors](#6-pipeline-chaining-processors)
7. [Workers & Runner: Orchestration](#7-workers--runner-orchestration)
8. [Transports: I/O Layer](#8-transports-io-layer)
9. [Services: AI Integrations](#9-services-ai-integrations)
10. [Context & Aggregation](#10-context--aggregation)
11. [Voice Activity Detection (VAD)](#11-voice-activity-detection-vad)
12. [Turn Management](#12-turn-management)
13. [The Bus: Inter-Worker Communication](#13-the-bus-inter-worker-communication)
14. [Observers: Non-Intrusive Monitoring](#14-observers-non-intrusive-monitoring)
15. [Serializers: Wire Formats](#15-serializers-wire-formats)
16. [Interruptions & Flow Control](#16-interruptions--flow-control)
17. [RTVI: Real-Time Voice Interface](#17-rtvi-real-time-voice-interface)
18. [Adapters: LLM Format Translation](#18-adapters-llm-format-translation)
19. [Multi-Worker Patterns](#19-multi-worker-patterns)
20. [Service Settings & Runtime Updates](#20-service-settings--runtime-updates)
21. [Testing Pipecat Code](#21-testing-pipecat-code)
22. [Contributing to Pipecat](#22-contributing-to-pipecat)
23. [The Pipecat Ecosystem](#23-the-pipecat-ecosystem)
24. [Deprecations & Migration Guide](#24-deprecations--migration-guide)
25. [Putting It All Together](#25-putting-it-all-together)
26. [Quick Reference: Key Patterns](#26-quick-reference-key-patterns)

---

## 1. What is Pipecat?

**Pipecat is an open-source Python framework for building real-time voice and multimodal conversational AI agents.**

Think of it like a factory assembly line:
- **Raw materials** (audio from a microphone, text from an LLM) enter as **Frames**
- **Workers on the line** (FrameProcessors) each do one job — transcribe audio, generate text, synthesize speech
- **The conveyor belt** (Pipeline) moves frames from one processor to the next
- **The factory manager** (PipelineWorker + WorkerRunner) starts everything, monitors health, and shuts down gracefully

```
┌─────────────────────────────────────────────────────────────────┐
│                        WorkerRunner                              │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                     PipelineWorker                         │  │
│  │  ┌─────────┐   ┌─────┐   ┌─────┐   ┌─────┐   ┌───────┐  │  │
│  │  │Transport│──▶│ STT │──▶│ LLM │──▶│ TTS │──▶│Transport│ │  │
│  │  │  Input  │   │     │   │     │   │     │   │ Output  │  │  │
│  │  └─────────┘   └─────┘   └─────┘   └─────┘   └───────┘  │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Project Structure

All source code lives under [`src/pipecat/`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/__init__.py).

```
src/pipecat/
├── frames/              # 📦 Frame definitions — the "data packets" of the system
│   └── frames.py        #    100+ frame types (audio, text, video, control, system)
│
├── processors/          # ⚙️  FrameProcessor base class + built-in processors
│   ├── frame_processor.py  # THE core abstraction — every component inherits this
│   └── aggregators/     #    Text/context aggregators (LLMContext, user response, etc.)
│
├── pipeline/            # 🔗 Pipeline assembly & orchestration
│   ├── pipeline.py      #    Chains processors into a linear pipeline
│   ├── worker.py        #    PipelineWorker — wraps pipeline for execution
│   ├── parallel_pipeline.py  # Run multiple pipelines in parallel
│   └── job_context.py   #    Job RPC context managers for inter-worker calls
│
├── workers/             # 👷 Worker model (multi-agent / multi-pipeline)
│   ├── base_worker.py   #    BaseWorker — lifecycle, bus, jobs
│   ├── runner.py        #    WorkerRunner — top-level entry point
│   ├── llm/             #    LLMWorker, LLMContextWorker (with @tool support)
│   └── ui/              #    UIWorker for frontend-connected agents
│
├── services/            # 🤖 60+ AI provider integrations
│   ├── ai_service.py    #    Base class for all AI services
│   ├── llm_service.py   #    LLM base (function calling, context summarization)
│   ├── stt_service.py   #    Speech-to-Text base (VAD integration, keepalive)
│   ├── tts_service.py   #    Text-to-Speech base (aggregation, audio contexts)
│   ├── openai/          #    OpenAI (GPT, Whisper, TTS, Realtime)
│   ├── anthropic/       #    Anthropic Claude
│   ├── google/          #    Google Gemini (including Gemini Live)
│   ├── deepgram/        #    Deepgram STT/TTS
│   ├── elevenlabs/      #    ElevenLabs TTS
│   ├── cartesia/        #    Cartesia TTS
│   └── ... (50+ more)   #    AWS, Azure, Groq, Together, Fireworks, etc.
│
├── transports/          # 🌐 External I/O (how audio/video enters and leaves)
│   ├── base_transport.py    # Abstract transport + TransportParams
│   ├── base_input.py        # BaseInputTransport (audio in, VAD, filtering)
│   ├── base_output.py       # BaseOutputTransport (audio out, mixing, timing)
│   ├── network/daily/       # Daily.co WebRTC transport
│   ├── network/livekit/     # LiveKit WebRTC transport
│   ├── network/websocket/   # WebSocket transport
│   ├── network/small_webrtc/# Lightweight WebRTC transport
│   └── local/               # Local mic/speaker transport
│
├── bus/                 # 📡 Inter-worker message bus
│   ├── bus.py           #    WorkerBus abstract interface
│   ├── messages.py      #    All bus message types (lifecycle, jobs, frames)
│   ├── local/           #    AsyncQueueBus (in-process)
│   └── network/         #    PgmqBus, RedisBus (distributed)
│
├── audio/               # 🔊 Audio processing utilities
│   ├── vad/             #    Voice Activity Detection (Silero, WebRTC)
│   ├── filters/         #    Audio filters (noise gate, Krisp, etc.)
│   ├── mixers/          #    Audio mixing for multi-stream output
│   └── turn/            #    Turn detection helpers
│
├── turns/               # 🗣️  User turn management
│   ├── user_turn_controller.py  # Coordinates start/stop strategies
│   ├── user_start/      #    Strategies for detecting user turn start
│   ├── user_stop/       #    Strategies for detecting user turn stop
│   └── user_idle_controller.py  # Idle timeout detection
│
├── observers/           # 👁️  Non-intrusive pipeline monitoring
│   ├── base_observer.py          # BaseObserver interface
│   ├── startup_timing_observer.py# Measures startup latency
│   └── user_bot_latency_observer.py # Measures conversation latency
│
├── serializers/         # 📋 Frame ↔ wire format conversion
│   ├── base_serializer.py  # FrameSerializer interface
│   ├── twilio.py        #    Twilio telephony protocol
│   ├── plivo.py         #    Plivo telephony protocol
│   └── ...              #    Vonage, Telnyx, Exotel, Genesys
│
├── registry/            # 📒 Worker registry (tracks ready workers)
├── adapters/            # 🔌 LLM adapter layer (OpenAI format ↔ provider format)
├── metrics/             # 📊 TTFB, latency, usage metrics
└── utils/               # 🔧 Helpers (async tasks, text processing, etc.)
```

**The key mental model**: Data flows as `Frame` objects through a chain of `FrameProcessor`s, wrapped in a `Pipeline`, managed by a `PipelineWorker`, and executed by a `WorkerRunner`.

---

## 3. The Foundation: BaseObject

📄 **Source**: [base_object.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/utils/base_object.py)

Every major class in Pipecat — processors, services, transports, observers — inherits from `BaseObject`. It provides three critical capabilities:

### 3.1 Unique Identity

Every object gets a unique integer ID and a human-readable name:

```python
# From base_object.py (lines 63-64)
self._id: int = obj_id()           # Global counter — every object is unique
self._name = name or f"{self.__class__.__name__}#{obj_count(self)}"
# Example: "OpenAILLMService#1", "Pipeline#3"
```

**Why this matters**: When debugging pipelines with dozens of processors, you need to know *which* processor logged a message. The auto-naming gives you that for free.

### 3.2 Event System

Pipecat uses a publish-subscribe event system. Any `BaseObject` can define events and let external code react:

```python
# STEP 1: A class registers an event type (usually in __init__)
self._register_event_handler("on_connected")          # async (default)
self._register_event_handler("on_first_audio", sync=True)  # sync = immediate

# STEP 2: User code attaches a handler
@my_service.event_handler("on_connected")
async def handle_connect(service, frame):
    print(f"{service.name} is connected!")

# STEP 3: The class fires the event when appropriate
await self._call_event_handler("on_connected", frame)
```

**Sync vs Async events** ([base_object.py L225-L240](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/utils/base_object.py#L225-L240)):
- **Async** (default): Handler runs in a background `asyncio.Task`. The caller doesn't wait — fire and forget.
- **Sync** (`sync=True`): Handler runs *immediately* inline. Must be fast — blocks the pipeline.

### 3.3 Task Management

Instead of raw `asyncio.create_task()`, always use:

```python
task = self.create_task(my_coroutine(), "descriptive_name")
await self.cancel_task(task, timeout=1.0)
```

**Why**: The `TaskManager` ([base_object.py L126-L154](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/utils/base_object.py#L126-L154)) tracks all tasks and cancels them during shutdown. Raw `asyncio.create_task()` creates orphaned tasks that can leak.

### 3.4 Lifecycle: `setup()` and `cleanup()`

```python
async def setup(self, task_manager):     # Wire up the task manager
    self._task_manager = task_manager

async def cleanup(self):                  # Wait for event tasks, release resources
    if self._event_tasks:
        await asyncio.wait(tasks)
```

**Key rule**: If your class owns child `BaseObject`s, override `setup()` and forward the task manager to them.

---

## 4. Frames: The Data Currency

📄 **Source**: [frames.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/frames/frames.py)

Frames are the single most important concept in Pipecat. **Every piece of data** — audio, text, images, control signals — travels through the pipeline as a Frame object.

### 4.1 The Frame Base Class

```python
# frames.py — The root of everything
@dataclass
class Frame:
    id: int = field(init=False)                     # Unique per frame (auto-assigned)
    name: str = field(init=False)                    # Auto-generated: "TextFrame#42"
    pts: int | None = field(init=False)              # Presentation timestamp (nanoseconds)
    broadcast_sibling_id: int | None = field(init=False)  # Paired frame ID when broadcast
    metadata: dict[str, Any] = field(init=False)     # Arbitrary metadata dictionary
    transport_source: str | None = field(init=False)       # Which transport created this
    transport_destination: str | None = field(init=False)   # Route to specific output

    def __post_init__(self):
        self.id = obj_id()
        self.name = f"{self.__class__.__name__}#{obj_count(self)}"
        self.pts = None
        self.broadcast_sibling_id = None
        self.metadata = {}
        self.transport_source = None
        self.transport_destination = None
```

**Key fields**:
- **`id`**: Every frame gets a unique ID. Set automatically via `__post_init__`, not passed to the constructor.
- **`pts`** (Presentation Timestamp): Nanosecond-precision timestamp for synchronizing audio with events like word highlights. The output transport uses this to schedule frame delivery.
- **`transport_destination`**: Routes frames to a specific output stream in multi-destination setups.
- **`broadcast_sibling_id`**: When a frame is broadcast in both directions via `broadcast_frame()`, each copy gets the other's ID here.

### 4.2 The Three Frame Families

Pipecat frames are organized into three families, each with different pipeline behavior:

```
Frame (base)
├── SystemFrame          ← HIGH PRIORITY, processed immediately by input task
│   ├── StartFrame
│   ├── CancelFrame
│   ├── InterruptionFrame
│   ├── ErrorFrame
│   ├── UserStartedSpeakingFrame
│   ├── UserStoppedSpeakingFrame
│   ├── InputAudioRawFrame
│   └── MetricsFrame, etc.
│
├── DataFrame            ← NORMAL PRIORITY, queued for serialized processing
│   ├── OutputAudioRawFrame / TTSAudioRawFrame
│   ├── TextFrame / LLMTextFrame / TranscriptionFrame
│   ├── OutputImageRawFrame
│   ├── LLMContextFrame
│   └── FunctionCallInProgressFrame, etc.
│
└── ControlFrame         ← NORMAL PRIORITY, processed in order like data frames
    ├── EndFrame (+ UninterruptibleFrame mixin)
    ├── StopFrame (+ UninterruptibleFrame mixin)
    ├── LLMFullResponseStartFrame / EndFrame
    ├── LLMThoughtStartFrame / EndFrame
    └── BotStartedSpeakingFrame, etc.
```

### 4.3 SystemFrame: The VIP Lane

📄 [frames.py — SystemFrame](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/frames/frames.py#L96-L103)

```python
@dataclass
class SystemFrame(Frame):
    """Frames that should be processed immediately, bypassing normal queues."""
```

**Why they exist**: Imagine audio is playing and the user interrupts. The `InterruptionFrame` must reach every processor *immediately* — it can't wait behind 500ms of buffered audio frames. SystemFrames get priority in the `FrameProcessorQueue` and are processed by the input task handler directly, bypassing the process queue entirely.

**Critical SystemFrame types**:

| Frame | Purpose | Direction |
|-------|---------|-----------|
| [`InterruptionFrame`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/frames/frames.py#L970) | Signal that the user interrupted | Downstream |
| [`CancelFrame`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/frames/frames.py#L884) | Hard-stop the entire pipeline | Downstream |
| [`StartFrame`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/frames/frames.py#L858) | Initialize all processors | Downstream |
| [`MetricsFrame`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/frames/frames.py#L1119) | Performance data (TTFB, etc.) | Upstream |
| [`ErrorFrame`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/frames/frames.py#L901) | Report errors upstream | Upstream |
| [`InputAudioRawFrame`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/frames/frames.py#L1246) | Raw audio from user's mic | Downstream |

### 4.4 Lifecycle Frames: Start, End, Stop, Cancel

These manage the pipeline's state machine: Start → Running → Stop/End/Cancel.

```python
@dataclass
class StartFrame(SystemFrame):               # ← NOTE: SystemFrame, not ControlFrame!
    """First frame sent — tells every processor to initialize."""
    audio_in_sample_rate: int = 16000
    audio_out_sample_rate: int = 24000        # Default is 24kHz, not 16kHz
    enable_metrics: bool = False
    enable_tracing: bool = False
    enable_usage_metrics: bool = False
    report_only_initial_ttfb: bool = False

@dataclass
class EndFrame(ControlFrame, UninterruptibleFrame):
    """Graceful shutdown — let processors finish current work, then clean up."""

@dataclass
class StopFrame(ControlFrame, UninterruptibleFrame):
    """Pause — stops processing but can be resumed with a new StartFrame."""

@dataclass
class CancelFrame(SystemFrame):
    """Hard shutdown — stop everything immediately, don't wait."""
```

**End vs Cancel**: `EndFrame` is a polite goodbye (finishes queued work, it's a `ControlFrame` so it waits in the queue). `CancelFrame` is an emergency stop (it's a `SystemFrame` so it skips the queue).

### 4.5 The Uninterruptible Mixin

📄 [frames.py — UninterruptibleFrame](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/frames/frames.py#L137-L148)

```python
@dataclass
class UninterruptibleFrame:
    """Mixin: frames that survive interruptions."""
```

When an interruption occurs, all pending frames in processor queues are **discarded** — except those marked `UninterruptibleFrame`. Examples: `EndFrame`, `StopFrame`. This ensures critical lifecycle signals are never lost. Note that `CancelFrame` doesn't need this mixin because it's a `SystemFrame` and is processed immediately, bypassing the queue that gets flushed.

### 4.6 Key Data Frames

| Frame | What it carries | Created by |
|-------|----------------|------------|
| [`InputAudioRawFrame`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/frames/frames.py#L1246) | Raw audio from user's mic | Transport Input |
| [`TranscriptionFrame`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/frames/frames.py#L416) | User's speech as text | STT Service |
| [`LLMContextFrame`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/frames/frames.py#L499) | Context (messages + tools) to send to LLM | Context Aggregator |
| [`TextFrame`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/frames/frames.py#L294) | Generic text (also base for LLMTextFrame) | LLM Service |
| [`TTSAudioRawFrame`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/frames/frames.py#L232) | Synthesized speech audio | TTS Service |
| [`OutputAudioRawFrame`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/frames/frames.py#L192) | Audio to send to user | Transport Output |

### 4.7 Frame Direction

Frames flow in two directions:
- **DOWNSTREAM** (left → right): Input data flowing toward output. Audio in → STT → LLM → TTS → Audio out.
- **UPSTREAM** (right → left): Acknowledgments, errors, metrics flowing back. ErrorFrame, UserStartedSpeakingFrame, etc.

```python
class FrameDirection(Enum):
    DOWNSTREAM = 1    # Input → Output (left to right)
    UPSTREAM = 2      # Output → Input (right to left)
```

---

## 5. FrameProcessor: The Processing Engine

📄 **Source**: [frame_processor.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/processors/frame_processor.py)

This is **the most important class in the entire framework**. Every component — STT, LLM, TTS, transports, aggregators — is a `FrameProcessor`. Understanding this class is understanding Pipecat.

### 5.1 What a FrameProcessor Does

A FrameProcessor is a node in the pipeline. It:
1. **Receives** frames from upstream (or downstream for upstream frames)
2. **Processes** them (transcribe audio, generate text, etc.)
3. **Pushes** results to the next processor

```python
class FrameProcessor(BaseObject):
    async def process_frame(self, frame: Frame, direction: FrameDirection):
        """Override this method to define what your processor does.
        
        IMPORTANT: The base implementation does NOT push frames through!
        It only handles system frames (StartFrame, InterruptionFrame,
        CancelFrame, Pause/Resume). Your override MUST push any frames
        you don't consume.
        """
        # Base handles: StartFrame, InterruptionFrame, CancelFrame,
        # FrameProcessorPauseFrame, FrameProcessorResumeFrame
        # Everything else is silently dropped unless you push it!
```

> **Critical rule**: If you override `process_frame`, always call `super().process_frame()` first (handles system frames), then push any unhandled frames yourself. If you don't push, the frame is **swallowed** and downstream processors never see it.

### 5.2 The Dual-Queue Architecture

📄 [frame_processor.py L246-L270](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/processors/frame_processor.py#L246-L270) (init), [L996-L1043](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/processors/frame_processor.py#L996-L1043) (task handlers)

Each processor has **two input paths** for different priority levels:

```
                    ┌────────────────────┐
  SystemFrame ─────▶│  __input_queue     │──▶ process_frame() immediately
                    │  (PriorityQueue)   │
                    └────────────────────┘

                    ┌────────────────────┐     ┌────────────────────┐
  DataFrame ───────▶│  __input_queue     │────▶│  __process_queue   │──▶ process_frame()
                    │  (normal priority) │     │  (FrameQueue)      │
                    └────────────────────┘     └────────────────────┘
```

**Why two queues?** System frames (interruptions, errors) must be processed immediately. Data frames wait their turn. The `__input_queue` is a `FrameProcessorQueue` (a `PriorityQueue`) that prioritizes SystemFrames. Two separate asyncio tasks handle each path:

```python
# Simplified from frame_processor.py L996-L1020
async def __input_frame_task_handler(self):
    """Handles ALL incoming frames — routes system vs non-system."""
    while True:
        (frame, direction, callback) = await self.__input_queue.get()

        if isinstance(frame, SystemFrame):
            # FAST PATH: Process immediately in this task
            await self.__process_frame(frame, direction, callback)
        else:
            # NORMAL PATH: Forward to the process queue for serialized handling
            await self.__process_queue.put((frame, direction, callback))

async def __process_frame_task_handler(self):
    """Handles non-system frames one at a time from the process queue."""
    while True:
        (frame, direction, callback) = await self.__process_queue.get()
        await self.__process_frame(frame, direction, callback)
```

### 5.3 Linking Processors Together

Processors form a doubly-linked list:

```python
# How Pipeline connects processors (simplified)
processor_a.link(processor_b)
# Internally:
#   processor_a._next = processor_b
#   processor_b._prev = processor_a
```

When a processor calls `push_frame()`:
- **DOWNSTREAM**: Frame goes to `self._next`
- **UPSTREAM**: Frame goes to `self._prev`

```python
async def push_frame(self, frame, direction=FrameDirection.DOWNSTREAM):
    if direction == FrameDirection.DOWNSTREAM:
        target = self._next        # Next processor in chain
    else:
        target = self._prev        # Previous processor in chain
    
    if target:
        await target.queue_frame(frame, direction)  # Put on target's input queue
```

### 5.4 The Processing Loop Lifecycle

```
                 StartFrame
                     │
    ┌────────────────▼────────────────────┐
    │  IDLE (waiting for StartFrame)       │
    └────────────────┬────────────────────┘
                     │ _start()
    ┌────────────────▼────────────────────┐
    │  RUNNING (processing frames)         │◄──── StopFrame (pause)
    └────────┬───────────────────┬────────┘       then StartFrame (resume)
             │                   │
         EndFrame            CancelFrame
             │                   │
    ┌────────▼───────┐  ┌───────▼────────┐
    │  STOPPING      │  │  CANCELLED     │
    │  (drain queue) │  │  (drop queue)  │
    └────────┬───────┘  └───────┬────────┘
             │                   │
    ┌────────▼───────────────────▼────────┐
    │  CLEANUP (release resources)         │
    └─────────────────────────────────────┘
```

### 5.5 Interruption Handling

📄 [frame_processor.py — _start_interruption](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/processors/frame_processor.py#L842-L867)

When an `InterruptionFrame` arrives (it's a `SystemFrame`, so it gets fast-path processing):

1. The **process task** is cancelled — all pending data/control frames are dropped
2. **Except** `UninterruptibleFrame`s (EndFrame, StopFrame) which are preserved in the queue
3. If the currently processing frame is `UninterruptibleFrame`, the task is NOT cancelled — only the queue is flushed
4. A **new** process task is created to handle any remaining uninterruptible frames

```python
# Simplified from frame_processor.py L842-L867
async def _start_interruption(self):
    current_is_uninterruptible = isinstance(
        self.__process_current_frame, UninterruptibleFrame
    )
    if current_is_uninterruptible:
        # Don't cancel the current frame, just flush non-uninterruptible from queue
        self.__reset_process_queue()
    else:
        # Cancel current work and start fresh
        await self.__cancel_process_task()
        self.__create_process_task()  # This also flushes the queue
```

To **trigger an interruption** from a processor, use `broadcast_interruption()`:
```python
await self.broadcast_interruption()  # Sends InterruptionFrame both up and downstream
```

> **Note**: The older `push_interruption_task_frame_and_wait()` method is deprecated since v0.0.104 and now delegates to `broadcast_interruption()`.

### 5.6 Writing a Custom Processor

Here's the pattern for creating your own processor:

```python
from pipecat.frames.frames import Frame, TextFrame
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor

class TextUppercaser(FrameProcessor):
    """Example: converts all text to uppercase."""
    
    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)  # ALWAYS call super first
        
        if isinstance(frame, TextFrame):
            # Transform the frame
            new_frame = TextFrame(text=frame.text.upper())
            await self.push_frame(new_frame, direction)
        else:
            # Pass through anything we don't handle
            await self.push_frame(frame, direction)
```

**Rules**:
1. Always call `super().process_frame()` first
2. Always push frames you don't handle (don't swallow them)
3. Push in the same direction the frame came from

---

## 6. Pipeline: Chaining Processors

📄 **Source**: [pipeline.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/pipeline/pipeline.py)

A Pipeline is itself a `FrameProcessor` that chains multiple processors into a linear sequence.

### 6.1 How Pipelines are Built

```python
from pipecat.pipeline.pipeline import Pipeline

pipeline = Pipeline([
    transport.input(),    # Processor 1: receives audio from WebRTC
    stt_service,          # Processor 2: transcribes audio to text
    llm_service,          # Processor 3: generates AI response
    tts_service,          # Processor 4: synthesizes speech
    transport.output(),   # Processor 5: sends audio to WebRTC
])
```

Internally, `Pipeline.__init__()` links processors in a chain:

```
┌────────┐    ┌──────┐    ┌──────┐    ┌──────┐    ┌─────────┐
│ Source  │───▶│ STT  │───▶│ LLM  │───▶│ TTS  │───▶│  Sink   │
│(hidden)│    │      │    │      │    │      │    │(hidden) │
└────────┘    └──────┘    └──────┘    └──────┘    └─────────┘
```

### 6.2 Source and Sink Processors

The Pipeline wraps your processors with two hidden processors:
- **Source** ([pipeline.py L25-L50](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/pipeline/pipeline.py#L25-L50)): Entry point. Routes frames into the chain.
- **Sink** ([pipeline.py L55-L80](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/pipeline/pipeline.py#L55-L80)): Exit point. Catches frames that fall off the end.

**Why hidden wrappers?** They handle:
1. Lifecycle management — `setup()` and `cleanup()` propagation to all child processors
2. Error handling at pipeline boundaries
3. Upstream/downstream routing at the edges

### 6.3 Pipeline is a Processor

Since `Pipeline` extends `FrameProcessor`, **pipelines can be nested**:

```python
inner_pipeline = Pipeline([processor_a, processor_b])
outer_pipeline = Pipeline([transport.input(), inner_pipeline, transport.output()])
```

### 6.4 ParallelPipeline

📄 **Source**: [parallel_pipeline.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/pipeline/parallel_pipeline.py)

Runs multiple pipeline branches simultaneously. A frame entering a ParallelPipeline is sent to **all branches** concurrently:

```python
parallel = ParallelPipeline(
    [Pipeline([stt_english])],     # Branch 1: English transcription
    [Pipeline([stt_spanish])],     # Branch 2: Spanish transcription
)
```

```
              ┌──── [STT English] ────┐
Input ───────▶│                        │───▶ Output (merged)
              └──── [STT Spanish] ────┘
```

---

## 7. Workers & Runner: Orchestration

### 7.1 PipelineWorker — Running a Pipeline

📄 **Source**: [worker.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/pipeline/worker.py)

A `PipelineWorker` is the runtime wrapper for a Pipeline. It handles:
- Sending the initial `StartFrame` to kick off the pipeline
- **Heartbeat monitoring** — detects if the pipeline stalls
- **Idle detection** — triggers callbacks when no frames flow for a period
- **Bus integration** — bridges pipeline frames to/from other workers

```python
from pipecat.pipeline.worker import PipelineWorker, PipelineParams
from pipecat.workers.runner import WorkerRunner

# Create the worker
worker = PipelineWorker(pipeline, params=PipelineParams(
    enable_heartbeats=True,          # Monitor pipeline health
    heartbeats_monitor_secs=10.0,    # Warn if heartbeat not received for 10s
))

# Run it
runner = WorkerRunner()
await runner.add_workers(worker)
await runner.run()
```

**Heartbeat mechanism** ([worker.py L1161-L1187](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/pipeline/worker.py#L1161-L1187)): The worker periodically pushes a `HeartbeatFrame` into the pipeline source. The sink processor catches it. If the heartbeat doesn't arrive within `heartbeats_monitor_secs`, the pipeline is assumed stuck.

### 7.2 BaseWorker — The Worker Contract

📄 **Source**: [base_worker.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/workers/base_worker.py)

`BaseWorker` is the abstract base class for all workers. `PipelineWorker` inherits from it.

**Key concepts**:

```python
class BaseWorker:
    # Lifecycle
    async def on_activated(self, args):    # Called when worker starts processing
    async def on_deactivated(self):        # Called when worker pauses

    # Bus communication  
    async def send_message(self, msg):     # Send to bus
    async def on_bus_message(self, msg):   # Receive from bus
    
    # Job handling
    @job(name="my_task")                   # Decorator for RPC-style calls
    async def handle_task(self, params):
        return {"result": "done"}
```

**Parent-child workers**: Workers can have children. When a parent worker ends, all its children are automatically cleaned up. This enables agent hierarchies.

### 7.3 WorkerRunner — The Top-Level Entry Point

📄 **Source**: [runner.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/workers/runner.py)

The `WorkerRunner` is where everything starts. It:
1. Creates and owns the `WorkerBus` (message bus)
2. Creates and owns the `WorkerRegistry` (tracks workers)
3. Handles OS signals (SIGINT/SIGTERM) for graceful shutdown
4. Manages the lifecycle of all registered workers

```python
runner = WorkerRunner()

# Register workers (can register multiple!)
await runner.add_workers(pipeline_worker_1, pipeline_worker_2)

# Start everything — blocks until all workers finish
await runner.run()
```

**Signal handling** ([runner.py L467+](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/workers/runner.py#L467)):
- **First SIGINT/SIGTERM**: Graceful shutdown — sends `EndFrame` to all pipelines
- **Second signal**: Hard cancel — sends `CancelFrame` immediately

**`auto_end` mode** ([runner.py L195-L234](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/workers/runner.py#L195-L234)):
- `auto_end=True` (default): Runner ends when all root workers finish. Good for single-session bots.
- `auto_end=False`: Runner stays alive. Good for servers (FastAPI) that create workers per session.

### 7.4 The Full Execution Stack

```
┌─────────────────────────────────────────┐
│             WorkerRunner                 │  ← Entry point, signal handling
│  ┌──────────────────────────────────┐   │
│  │         WorkerBus                 │   │  ← Inter-worker messaging
│  └──────────────────────────────────┘   │
│  ┌──────────────────────────────────┐   │
│  │       WorkerRegistry             │   │  ← Tracks all workers
│  └──────────────────────────────────┘   │
│  ┌──────────────────────────────────┐   │
│  │       PipelineWorker              │   │  ← Manages one pipeline
│  │  ┌────────────────────────────┐  │   │
│  │  │         Pipeline            │  │   │  ← Chain of processors
│  │  │  [Input] → [STT] → [LLM]  │  │   │
│  │  │  → [TTS] → [Output]       │  │   │
│  │  └────────────────────────────┘  │   │
│  └──────────────────────────────────┘   │
└─────────────────────────────────────────┘
```

---

## 8. Transports: I/O Layer

Transports are how audio/video **enters and exits** the pipeline. They bridge the gap between external communication protocols (WebRTC, WebSocket, local mic) and the frame-based pipeline.

### 8.1 BaseTransport

📄 **Source**: [base_transport.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/transports/base_transport.py)

```python
class BaseTransport(BaseObject):
    def input(self) -> FrameProcessor:    # Returns the input processor
    def output(self) -> FrameProcessor:   # Returns the output processor
```

A transport provides **two processors** — one for input, one for output. You place them at the edges of your pipeline:

```python
pipeline = Pipeline([
    transport.input(),    # ← Input processor (start of pipeline)
    # ... your processors ...
    transport.output(),   # ← Output processor (end of pipeline)
])
```

### 8.2 TransportParams

📄 [base_transport.py — TransportParams](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/transports/base_transport.py#L25-L89)

```python
class TransportParams(BaseModel):
    audio_out_enabled: bool = False          # Enable audio output
    audio_out_sample_rate: int | None = None # Output sample rate (Hz)
    audio_in_enabled: bool = False           # Enable audio input
    audio_in_sample_rate: int | None = None  # Input sample rate (Hz)
    audio_in_filter: BaseAudioFilter | None  # Apply noise filtering
    audio_in_passthrough: bool = True        # Push raw audio downstream
    video_in_enabled: bool = False           # Enable video input
    video_out_enabled: bool = False          # Enable video output
    audio_out_mixer: BaseAudioMixer | None   # Mix multiple audio streams
```

### 8.3 BaseInputTransport

📄 **Source**: [base_input.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/transports/base_input.py)

Receives raw audio/video from external sources and pushes `InputAudioRawFrame` / `InputImageRawFrame` downstream.

**Key behavior**:
- Runs an async audio task that reads from an internal queue with a 0.5s timeout
- Applies audio filters (noise gate, Krisp) before pushing frames
- Tracks user/bot speaking state for interruption logic
- Can be **paused** via `StopFrame` (stops pushing frames but keeps receiving)

### 8.4 BaseOutputTransport & MediaSender

📄 **Source**: [base_output.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/transports/base_output.py)

The output transport is more complex because it must handle:
- **Audio chunking**: Splits large audio frames into 10ms×N chunks for smooth playback
- **Audio resampling**: Converts sample rates to match the transport
- **Audio mixing**: Merges multiple audio streams (background music + speech)
- **Bot speaking detection**: Tracks when the bot is speaking for interruption handling
- **Multi-destination output**: Separate `MediaSender` per output destination

```
OutputAudioRawFrame ──▶ MediaSender ──▶ Audio chunk buffer ──▶ write_audio_frame()
                           │
                           ├── Bot speaking detection (BotStartedSpeakingFrame)
                           ├── Audio mixing (if mixer configured)
                           └── Resampling (if sample rates differ)
```

**MediaSender** ([base_output.py L388-L460](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/transports/base_output.py#L388-L460)): Each output destination gets its own MediaSender with independent audio/video/clock tasks.

### 8.5 Available Transports

| Transport | Protocol | Use Case |
|-----------|----------|----------|
| Daily | WebRTC | Production voice/video agents |
| LiveKit | WebRTC | Alternative WebRTC platform |
| WebSocket | WebSocket | Telephony (Twilio, Plivo, etc.) |
| SmallWebRTC | WebRTC | Lightweight self-hosted WebRTC |
| Local | OS audio | Development/testing with local mic |

---

## 9. Services: AI Integrations

📄 **Source**: [ai_service.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/services/ai_service.py)

Services are FrameProcessors that wrap AI provider APIs. They follow an inheritance hierarchy:

```
FrameProcessor
└── AIService                  # Base: settings management, lifecycle
    ├── LLMService             # Language models (chat completion, function calling)
    ├── STTService             # Speech-to-Text (audio → text)
    ├── TTSService             # Text-to-Speech (text → audio)
    ├── VisionService          # Image understanding
    └── ImageGenService        # Image generation
```

### 9.1 AIService Base

📄 [ai_service.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/services/ai_service.py#L30-L100)

Provides **settings management** for all AI services:

```python
class AIService(FrameProcessor):
    async def process_frame(self, frame, direction):
        if isinstance(frame, UpdateSettingsFrame):
            await self._update_settings(frame.settings)  # Partial update
        elif isinstance(frame, SetSettingsFrame):
            await self._set_settings(frame.settings)      # Full replace
```

Services use `ServiceSettings` ([settings.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/services/settings.py)) with runtime validation and delta support — you can update just one parameter without replacing the entire config.

### 9.2 LLMService

📄 **Source**: [llm_service.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/services/llm_service.py)

The most complex service type. Handles:

- **Streaming token output**: LLM generates text token-by-token, each pushed as a `TextFrame`
- **Function/tool calling**: Registers functions, executes them, returns results to the LLM
- **Context management**: Works with `LLMContext` to maintain conversation history
- **Parallel tool execution**: Can run multiple function calls simultaneously

```
LLMContextFrame ──▶ LLMService ──▶ LLMFullResponseStartFrame
                                 ──▶ LLMTextFrame (token by token)
                                 ──▶ LLMFullResponseEndFrame
                                 ──▶ FunctionCallInProgressFrame
                                 ──▶ FunctionCallResultFrame
```

**Mandatory frame sequence**: Every LLM response **must** emit this sequence:
1. `LLMFullResponseStartFrame` — Signals the start of an LLM response
2. `LLMTextFrame` (one per token) — Contains streamed content
3. `LLMFullResponseEndFrame` — Signals the end of an LLM response

For **reasoning models** (chain-of-thought), emit thought frames before the response:
- `LLMThoughtStartFrame` → `LLMThoughtTextFrame`(s) → `LLMThoughtEndFrame`

**The adapter pattern** ([adapters/](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/adapters)): Non-OpenAI LLM services declare an `adapter_class` to translate the universal OpenAI message format to their native format. For example, `AnthropicLLMService` uses `AnthropicLLMAdapter`. See [Section 18](#18-adapters-llm-format-translation) for details.

**Function calling flow** ([llm_service.py — process_frame L557](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/services/llm_service.py#L557), [register_function L762](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/services/llm_service.py#L762)):
1. LLM returns a tool call request
2. Service looks up the registered function handler
3. Pushes `FunctionCallInProgressFrame` downstream
4. Executes the handler (with optional timeout)
5. Pushes `FunctionCallResultFrame` and feeds result back to LLM

### 9.3 STTService

📄 **Source**: [stt_service.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/services/stt_service.py)

Converts audio to text. Key features:
- **VAD integration**: Only processes audio when someone is speaking
- **Keepalive**: Sends periodic pings to keep WebSocket connections alive
- **TTFB tracking**: Measures time from audio input to first transcription result
- **Audio segmentation**: Buffers audio and sends chunks to the STT API

### 9.4 TTSService

📄 **Source**: [tts_service.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/services/tts_service.py)

Converts text to audio. Key features:
- **Text aggregation modes**: Sentence-level (waits for complete sentences) or Token-level (sends immediately)
- **Word timestamps**: Tracks which word is being spoken for lip sync / highlights
- **Serialization queue**: Orders audio frames with PTS timestamps for synchronized output
- **Push-to-talk support**: Can pause/resume audio generation

### 9.5 Available Providers (60+)

| Category | Providers |
|----------|-----------|
| **LLM** | OpenAI, Anthropic, Google Gemini, Groq, Fireworks, Together, DeepSeek, Ollama, OpenRouter, Cerebras, etc. |
| **STT** | Deepgram, AssemblyAI, Google, Azure, Whisper, Gladia, Speechmatics, Cartesia, ElevenLabs, NVIDIA, etc. |
| **TTS** | ElevenLabs, Cartesia, Google, Azure, LMNT, Rime, Kokoro, Fish, Deepgram, OpenAI, NVIDIA, etc. |
| **S2S** | OpenAI Realtime, Gemini Multimodal Live, AWS Nova Sonic, Grok Voice Agent, Ultravox |
| **Vision** | Moondream, Google, OpenAI |
| **Image** | Fal, Google Imagen |
| **Video** | HeyGen, Tavus, Simli, LemonSlice |
| **Memory** | mem0 |
| **Analytics** | OpenTelemetry, Sentry |
| **Audio Processing** | Silero VAD, Krisp Viva, Koala, ai-coustics, RNNoise |

---

## 10. Context & Aggregation

### 10.1 LLMContext — Conversation Memory

📄 **Source**: [llm_context.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/processors/aggregators/llm_context.py)

`LLMContext` is the **conversation memory** — it stores the message history sent to the LLM.

```python
context = LLMContext(
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Hello!"},
    ],
    tools=ToolsSchema(standard_tools=[...]),   # Available tools
    tool_choice="auto",                         # Let LLM decide
)
```

**Key design decision**: Messages use OpenAI's format as the universal format. When sending to non-OpenAI providers (Anthropic, Google), an **adapter** translates on-the-fly.

**Multimedia support**: LLMContext can encode images and audio directly into messages:
```python
await context.add_image_frame_message(format="RGB", size=(640,480), image=raw_bytes)
await context.add_audio_frames_message(audio_frames=[audio_frame_1, audio_frame_2])
```

### 10.2 Context Aggregator Pair

The aggregator pair manages the **turn-taking** between user and assistant:

```
┌────────────────┐                      ┌──────────────────┐
│  User          │                      │  Assistant        │
│  Aggregator    │───▶ [LLM] ─────────▶│  Aggregator       │
│                │                      │                   │
│ Collects user  │                      │ Collects LLM      │
│ transcriptions │                      │ response tokens   │
│ into context   │                      │ into context      │
└────────────────┘                      └──────────────────┘
```

- **User Aggregator**: Accumulates `TranscriptionFrame`s until the user stops speaking, then packages them as a user message and triggers the LLM
- **Assistant Aggregator**: Accumulates `TextFrame`s from the LLM response, then packages them as an assistant message in the context

This creates a clean conversation history: `[system, user, assistant, user, assistant, ...]`

### 10.3 Realtime Service Mode

When using **Speech-to-Speech** (S2S) services like OpenAI Realtime, Gemini Live, or AWS Nova Sonic, the standard context aggregation flow doesn't apply because the service handles both STT and LLM internally. For these cases, use `realtime_service_mode`:

```python
context_aggregator = llm.create_context_aggregator(
    context,
    realtime_service_mode=True,  # Tailor aggregation for S2S services
)
```

**What `realtime_service_mode=True` does**:

1. **Decouples context writes from `UserStoppedSpeakingFrame`** — Instead, the assistant response start triggers user message writes. This ensures proper context even when the S2S service provides no turn frames and local VAD is disabled.

2. **Lets `UserStoppedSpeakingFrame` fire without waiting for transcripts** — Reduces latency when local turn detection drives the S2S service conversation.

3. **Auto-selects turn strategies** — For services that emit their own turn frames (OpenAI Realtime, Azure, Grok, Inworld), external turn strategies are swapped in automatically. For services that don't (Gemini Live, Nova Sonic, Ultravox), defaults stay in place for locally-driven turn detection.

> **Important**: With `realtime_service_mode=True`, listen for `on_user_turn_message_added` instead of `on_user_turn_stopped` to get the finalized user message.

### 10.4 App Resources — Sharing State Across the Pipeline

📄 **Source**: [worker.py — app_resources](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/pipeline/worker.py)

`app_resources` is a dictionary you attach to the `PipelineWorker` for sharing state across function call handlers and custom processors:

```python
worker = PipelineWorker(
    pipeline,
    app_resources={"db": database_connection, "config": app_config},
)

# In a function call handler:
async def get_weather(params: FunctionCallParams):
    db = params.app_resources["db"]   # Access shared resources
    return {"result": await db.query(...)}

# In a custom FrameProcessor:
class MyProcessor(FrameProcessor):
    async def process_frame(self, frame, direction):
        db = self.pipeline_worker.app_resources["db"]
```

> **Note**: The old name `tool_resources` is deprecated as of 1.2.0. Use `app_resources` instead.

---

## 11. Voice Activity Detection (VAD)

📄 **Source**: [vad_analyzer.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/audio/vad/vad_analyzer.py)

VAD determines **when someone is speaking**. It's the foundation of natural conversation — without it, the bot wouldn't know when to listen and when to respond.

### 11.1 The VAD State Machine

```
          voice detected          confirmed (start_secs elapsed)
  QUIET ──────────────▶ STARTING ──────────────────────────────▶ SPEAKING
    ▲                      │                                        │
    │                      │ voice stops (false alarm)              │ voice stops
    │                      ▼                                        ▼
    └──────────────────── QUIET                                 STOPPING
    ▲                                                               │
    │                    confirmed (stop_secs elapsed)               │
    └───────────────────────────────────────────────────────────────┘
```

📄 [vad_analyzer.py L189-L243](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/audio/vad/vad_analyzer.py#L189-L243)

### 11.2 VADParams

```python
class VADParams(BaseModel):
    confidence: float = 0.7    # Min confidence to count as speech
    start_secs: float = 0.2    # How long to wait before confirming speech start
    stop_secs: float = 0.2     # How long to wait before confirming speech stop
    min_volume: float = 0.6    # Min audio volume threshold
```

**Why debouncing?** Without `start_secs`/`stop_secs`, brief noises would trigger false starts, and short pauses between words would trigger false stops. The debouncing creates stable transitions.

### 11.3 How VAD Works Internally

1. Audio arrives as raw bytes
2. Audio is buffered until we have enough frames (e.g., 30ms worth for Silero)
3. The buffer is sent to the ML model (runs in a `ThreadPoolExecutor` to avoid blocking)
4. Model returns a confidence score (0.0 to 1.0)
5. Score + volume are checked against thresholds
6. State machine transitions based on consecutive frames above/below threshold

**Available VAD implementations**: Silero (neural network, most accurate) and WebRTC VAD (lightweight, faster)

---

## 12. Turn Management

📄 **Source**: [turns/](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/turns/user_turn_controller.py)

Turn management answers: **"When does the user start/stop talking, and what should happen?"**

### 12.1 User Turn Strategies

Pipecat uses a **strategy pattern** for turn detection:

```
┌─────────────────────────────────┐
│      UserTurnController          │
│  ┌───────────────────────────┐  │
│  │  Start Strategy            │  │ ← "When does the user START talking?"
│  │  (e.g., VADUserTurnStart)  │  │    Pushes UserStartedSpeakingFrame
│  └───────────────────────────┘  │
│  ┌───────────────────────────┐  │
│  │  Stop Strategy             │  │ ← "When does the user STOP talking?"
│  │  (e.g., VADUserTurnStop)   │  │    Pushes UserStoppedSpeakingFrame
│  └───────────────────────────┘  │
└─────────────────────────────────┘
```

📄 [user_turn_strategies.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/turns/user_turn_strategies.py)

**Start strategies** ([user_start/](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/turns/user_start)):
- `VADUserTurnStartStrategy`: Uses VAD to detect speech onset → triggers interruption
- Custom strategies can be written for button-press-to-talk, etc.

**Stop strategies** ([user_stop/](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/turns/user_stop)):
- `VADUserTurnStopStrategy`: Uses VAD silence detection
- Can be combined with LLM-based endpoint detection

### 12.2 Interruption Flow

When the user starts speaking while the bot is still talking:

```
1. VAD detects voice    →  UserStartedSpeakingFrame
2. Turn start strategy  →  InterruptionFrame (sent downstream)
3. Output transport     →  Cancels current audio output
4. TTS service          →  Stops generating audio
5. LLM service          →  Cancels current generation
6. User finishes        →  UserStoppedSpeakingFrame
7. Transcription        →  New user message added to context
8. LLM                  →  Generates new response
```

### 12.3 Idle Detection

📄 [user_idle_controller.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/turns/user_idle_controller.py)

Detects when the user hasn't spoken for a configurable period. Useful for:
- Prompting "Are you still there?"
- Auto-ending idle sessions

---

## 13. The Bus: Inter-Worker Communication

📄 **Source**: [bus/](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/bus/bus.py) | [messages.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/bus/messages.py)

When you have **multiple workers** (multi-agent setups), they communicate through the `WorkerBus`.

### 13.1 Bus Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Worker A    │     │   Worker B    │     │   Worker C    │
│  (Greeter)    │     │  (Specialist) │     │   (Logger)    │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                     │                     │
       └─────────────────────┼─────────────────────┘
                             │
                    ┌────────▼────────┐
                    │   WorkerBus      │
                    │  (pub/sub)       │
                    └─────────────────┘
```

### 13.2 Message Types

📄 [messages.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/bus/messages.py)

All messages extend `BusMessage` with `source` and `target` fields. Two priority levels:

| Priority | Base Class | Delivery |
|----------|-----------|----------|
| Normal | `BusDataMessage` | FIFO queue |
| High | `BusSystemMessage` | Preempts data messages |

**Key message types**:

| Message | Purpose |
|---------|---------|
| `BusFrameMessage` | Wraps a pipeline Frame for transport between workers |
| `BusActivateWorkerMessage` | Tell a worker to start processing |
| `BusDeactivateWorkerMessage` | Tell a worker to pause |
| `BusEndMessage` | Request graceful session end |
| `BusCancelMessage` | Request hard cancel (high priority) |
| `BusJobRequestMessage` | RPC: request work from another worker |
| `BusJobResponseMessage` | RPC: return results |
| `BusWorkerReadyMessage` | Announce worker is ready |

### 13.3 Bus Implementations

| Implementation | Use Case |
|---------------|----------|
| `AsyncQueueBus` (local) | In-process, same Python process |
| `PgmqBus` (network) | Distributed via PostgreSQL message queue |
| `RedisBus` (network) | Distributed via Redis pub/sub |

### 13.4 BusBridgeProcessor

📄 [bridge_processor.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/bus/bridge_processor.py)

Connects a pipeline to the bus. Frames entering the bridge are serialized and sent to other workers. Frames arriving from the bus are deserialized and injected into the local pipeline. This is how `bridged=True` workers exchange frames.

---

## 14. Observers: Non-Intrusive Monitoring

📄 **Source**: [base_observer.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/observers/base_observer.py)

Observers let you **watch** frames flowing through the pipeline without modifying the pipeline itself. They're attached to the `PipelineWorker`, not inserted into the processor chain.

### 14.1 BaseObserver Interface

```python
class BaseObserver(BaseObject):
    async def on_process_frame(self, data: FrameProcessed):
        """Called when ANY processor processes a frame."""
        pass

    async def on_push_frame(self, data: FramePushed):
        """Called when a frame is pushed from one processor to another."""
        pass
    
    async def on_pipeline_started(self):
        """Called after StartFrame has been processed by all processors."""
        pass
```

### 14.2 Built-in Observers

| Observer | Purpose |
|----------|---------|
| [StartupTimingObserver](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/observers/startup_timing_observer.py) | Measures how long each processor takes to start up |
| [UserBotLatencyObserver](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/observers/user_bot_latency_observer.py) | Measures end-to-end conversation latency |
| [TurnTrackingObserver](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/observers/turn_tracking_observer.py) | Tracks conversation turns for analytics |

### 14.3 Using Observers

```python
worker = PipelineWorker(
    pipeline,
    observers=[
        StartupTimingObserver(),
        UserBotLatencyObserver(),
    ],
)
```

**Why observers vs. adding a processor?** Observers see **all frames across all processors**. A processor only sees frames at its position in the chain. Observers are ideal for cross-cutting concerns like logging and metrics.

---

## 15. Serializers: Wire Formats

📄 **Source**: [base_serializer.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/serializers/base_serializer.py)

Serializers convert frames to/from wire formats for WebSocket-based transports. Each telephony provider has its own protocol.

### 15.1 FrameSerializer Interface

```python
class FrameSerializer(BaseObject):
    def serialize(self, frame: Frame) -> bytes | None:
        """Convert a frame to bytes for transmission."""
    
    def deserialize(self, data: bytes | str) -> Frame | None:
        """Convert received bytes back to a frame."""
```

### 15.2 Available Serializers

| Serializer | Provider | Encoding |
|-----------|----------|----------|
| [TwilioFrameSerializer](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/serializers/twilio.py) | Twilio | μ-law 8kHz |
| [PlivoFrameSerializer](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/serializers/plivo.py) | Plivo | μ-law/PCM |
| [VonageFrameSerializer](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/serializers/vonage.py) | Vonage | PCM 16kHz |
| [TelnyxFrameSerializer](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/serializers/telnyx.py) | Telnyx | μ-law/PCM |
| [ExotelFrameSerializer](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/serializers/exotel.py) | Exotel | μ-law |
| [GenesysFrameSerializer](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/serializers/genesys.py) | Genesys | Various |

Serializers handle the details of provider-specific WebSocket message formats, audio encoding (μ-law, PCM), sample rates, and metadata extraction.

---

## 16. Interruptions & Flow Control

Interruptions are critical for natural conversation. Without them, the bot would talk over the user.

### 16.1 How Interruptions Work

```
User speaks ──▶ VAD detects voice ──▶ UserStartedSpeakingFrame (upstream)
                                            │
                                            ▼
                                   Turn Start Strategy
                                            │
                                            ▼
                                   InterruptionFrame (downstream)
                                            │
                    ┌───────────────────────┤
                    ▼                       ▼
            FrameProcessor            OutputTransport
            (flush queue)          (cancel audio playback)
```

### 16.2 What Happens During an Interruption

1. **Processing queues are flushed** — pending frames are discarded (except UninterruptibleFrames)
2. **Output transport restarts** — MediaSender cancels audio/clock tasks and creates fresh ones
3. **Bot stopped speaking** — `BotStoppedSpeakingFrame` is pushed both up and downstream
4. **LLM cancels** — if the LLM was mid-generation, the response is abandoned
5. **TTS cancels** — any queued text-to-speech is dropped

### 16.3 Broadcasting Interruptions

📄 [frame_processor.py — broadcast_interruption](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/processors/frame_processor.py#L718-L723)

Any processor can trigger an interruption by calling `broadcast_interruption()`:

```python
await self.broadcast_interruption()
# This:
#   1. Resets the process task (flushes non-uninterruptible frames)
#   2. Stops all active metrics
#   3. Broadcasts InterruptionFrame in BOTH directions (up and downstream)
```

The `InterruptionFrame` itself is a simple empty `SystemFrame` — it carries no special fields. The interruption behavior is implemented by `_start_interruption()` in each processor that receives it (see [§5.5](#55-interruption-handling)).

### 16.4 StopFrame vs EndFrame vs CancelFrame

| Frame | Type | Behavior |
|-------|------|----------|
| `StopFrame` | Control | **Pause** — stops processing but can be resumed with a new `StartFrame` |
| `EndFrame` | Control | **Graceful end** — processes remaining queued frames, then shuts down |
| `CancelFrame` | System | **Hard stop** — drops everything immediately, skips queues |

---

## 17. RTVI: Real-Time Voice Interface

📄 **Source**: [rtvi.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/processors/frameworks/rtvi.py)

RTVI (Real-Time Voice Interface) is a **protocol** bridging web clients and the pipeline. It lets front-end apps communicate with a Pipecat bot using structured messages instead of raw audio.

### 17.1 Two Sides of RTVI

| Component | Role |
|-----------|------|
| `RTVIProcessor` | **Inbound**: Handles client messages (text input, audio, function call results, UI events) → pushes pipeline frames |
| `RTVIObserver` | **Outbound**: Watches pipeline frames → converts to client messages (speaking events, transcriptions, LLM lifecycle, metrics) |

```
Browser (client-js/client-react)
        │                    ▲
        │ RTVI messages      │ RTVI messages
        ▼                    │
┌──────────────┐    ┌───────────────┐
│ RTVIProcessor │    │ RTVIObserver   │    ← Attached to PipelineWorker
│  (inbound)    │    │  (outbound)    │
└──────┬───────┘    └───────┬───────┘
       │ Pipeline Frames     │ Pipeline Frames
       ▼                     │
   [Pipeline] ───────────────┘
```

### 17.2 UI Agent Protocol

RTVI includes a **UI Agent Protocol** for voice agents that can see and drive a web UI:

**Client → Server messages**:
- `ui-event` — User action (click, scroll, etc.)
- `ui-snapshot` — Accessibility snapshot of the current page
- `ui-cancel-job-group` — Cancel a running job group

**Server → Client messages**:
- `ui-command` — Drive the UI (`scroll_to`, `highlight`, `click`, `set_input_value`, `select_text`)
- `ui-job-group` — Stream progress of long-running job groups

These are used by the [`UIWorker`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/workers/ui) to build voice agents grounded in what the user is looking at.

### 17.3 RTVI Versioning

The protocol version is tracked as `PROTOCOL_VERSION` in `rtvi.py`. The current version is `1.3.0`. Client SDKs negotiate this version during the initial handshake.

---

## 18. Adapters: LLM Format Translation

📄 **Source**: [adapters/](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/adapters)

Pipecat uses **OpenAI's message format** as the universal internal representation. When sending to non-OpenAI providers, an **adapter** translates on-the-fly.

### 18.1 Why Adapters?

Without adapters, every LLM service would need to implement its own message formatting. Instead:

```
                    Universal Format (OpenAI)
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
       ┌──────────┐ ┌──────────┐ ┌──────────┐
       │ OpenAI   │ │ Anthropic│ │ Google   │
       │ Adapter  │ │ Adapter  │ │ Adapter  │
       │(identity)│ │ (convert)│ │ (convert)│
       └──────────┘ └──────────┘ └──────────┘
```

### 18.2 BaseLLMAdapter Interface

📄 [base_llm_adapter.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/adapters/base_llm_adapter.py)

```python
class BaseLLMAdapter:
    def get_llm_invocation_params(self, context):
        """Extract provider-specific params from universal context."""

    def to_provider_tools_format(self, tools_schema):
        """Convert standard tools schema to provider format."""

    def get_messages_for_logging(self, context):
        """Format messages for human-readable logging."""
```

Each non-OpenAI LLM service declares its adapter via the `adapter_class` class attribute:

```python
class AnthropicLLMService(LLMService):
    adapter_class = AnthropicLLMAdapter   # Handles Claude's message format
```

Available adapters: `OpenAILLMAdapter` (default/identity), `AnthropicLLMAdapter`, `GeminiLLMAdapter`, `BedrockLLMAdapter`, and more under [`src/pipecat/adapters/services/`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/adapters/services).

---

## 19. Multi-Worker Patterns

Pipecat's multi-worker system lets you build **multi-agent applications** where specialized workers cooperate. Every `PipelineWorker` is a peer on a shared bus.

### 19.1 Pattern Overview

| Pattern | Description | Example |
|---------|-------------|---------|
| **Handoff** | Two LLM workers swap "who is talking" | Greeter → Support Agent |
| **Parallel Fan-Out** | One worker dispatches to multiple peers | Three analysts debate a topic |
| **Sidecar** | Main pipeline talks to a long-lived peer | Voice bot + code assistant |
| **Distributed** | Workers across separate processes/machines | Redis or PGMQ bus |
| **Point-to-Point Proxy** | WebSocket bridge between local and remote | No shared bus needed |
| **UI Worker** | Worker drives a web client over RTVI | Voice-guided form filling |

### 19.2 Handoff Pattern

📄 **Example**: [local-handoff/](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/examples/multi-worker/local-handoff)

Two LLM workers share a transport pipeline. A `transfer_to_worker` tool lets the active worker hand control to a peer:

```python
# Worker A (Greeter) — handles initial conversation
greeter = LLMContextWorker(
    name="greeter",
    llm=OpenAILLMService(model="gpt-4o"),
    context=LLMContext(messages=[
        {"role": "system", "content": "You are a friendly greeter. Transfer to support for technical questions."},
    ]),
)

# Worker B (Support) — handles technical questions
support = LLMContextWorker(
    name="support",
    llm=OpenAILLMService(model="gpt-4o"),
    context=LLMContext(messages=[
        {"role": "system", "content": "You are a technical support specialist."},
    ]),
)
```

### 19.3 Parallel Fan-Out Pattern (Job Groups)

📄 **Example**: [parallel-debate/](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/examples/multi-worker/parallel-debate)

One worker dispatches work to several peers simultaneously using `job_group()`:

```python
# Main worker fans out to three specialists
async with self.job_group("advocate", "critic", "analyst") as group:
    results = await group.send({"question": user_question})
    # All three run in parallel, results collected when all finish
```

### 19.4 The `@job` Decorator

📄 **Source**: [job_decorator.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/pipeline/job_decorator.py)

Workers expose RPC-style handlers with `@job`:

```python
class MyWorker(BaseWorker):
    @job(name="research", sequential=True)  # Process one at a time
    async def handle_research(self, params):
        # Do work...
        return {"answer": "..."}
```

Callers invoke jobs with:

```python
async with self.job("my_worker", name="research") as job:
    result = await job.send({"query": "..."})
```

### 19.5 The `@tool` Decorator (LLMWorker)

📄 **Source**: [tool_decorator.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/workers/llm/tool_decorator.py)

On an `LLMWorker`, methods marked `@tool` are auto-collected and registered with the LLM service:

```python
class MyAgent(LLMWorker):
    @tool(cancel_on_interruption=True, timeout=30)
    async def get_weather(self, city: str) -> str:
        """Get current weather for a city."""
        return f"72°F in {city}"
```

### 19.6 Specialized Worker Types

| Worker | Base | Purpose |
|--------|------|---------|
| [`BaseWorker`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/workers/base_worker.py) | — | Abstract base: lifecycle, bus, jobs |
| [`PipelineWorker`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/pipeline/worker.py) | `BaseWorker` | Wraps a pipeline + transport |
| [`LLMWorker`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/workers/llm) | `BaseWorker` | Adds `@tool` auto-collection |
| [`LLMContextWorker`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/workers/llm) | `LLMWorker` | Adds `LLMContext` + aggregator pair |
| [`UIWorker`](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/workers/ui) | `LLMContextWorker` | Drives web UI via RTVI snapshots |

### 19.7 Distributed Workers

For running workers across processes or machines, swap the bus implementation:

```python
from pipecat.bus.network.redis import RedisBus

runner = WorkerRunner(bus=RedisBus(url="redis://localhost:6379"))
```

Available network bus implementations:
- **RedisBus** ([bus/network/redis](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/bus/network)) — Redis pub/sub
- **PgmqBus** ([bus/network/pgmq](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/bus/network)) — PostgreSQL message queue (Supabase-friendly)

### 19.8 Worker Registry & `@worker_ready`

📄 **Source**: [registry/](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/registry)

The `WorkerRegistry` tracks which workers are active. Use `watch()` or the `@worker_ready` decorator to be notified when a named worker becomes available:

```python
from pipecat.pipeline.worker_ready_decorator import worker_ready

@worker_ready(name="specialist")
async def on_specialist_ready(self, worker_info):
    print(f"Specialist worker is ready: {worker_info}")
```

---

## 20. Service Settings & Runtime Updates

📄 **Source**: [settings.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/services/settings.py)

Every AI service exposes a **Settings dataclass** that serves two roles:

### 20.1 Store Mode vs Delta Mode

```python
from pipecat.services.settings import TTSSettings, NOT_GIVEN

@dataclass
class MyTTSSettings(TTSSettings):
    """Settings for MyTTS service.

    Parameters:
        speaking_rate: Speed multiplier (0.5–2.0).
    """
    speaking_rate: float | None = field(default_factory=lambda: NOT_GIVEN)
```

- **Store mode**: `self._settings` holds real values for every field
- **Delta mode**: An update frame specifies only fields that should change; unset fields remain `NOT_GIVEN`

### 20.2 Wiring Settings into `__init__`

```python
class MyTTSService(TTSService):
    Settings = MyTTSSettings
    _settings: Settings

    def __init__(self, *, api_key: str, settings=None, **kwargs):
        # 1. Defaults — every field has a real value (store mode)
        default_settings = self.Settings(
            model="my-model-v1", voice="default-voice",
            language="en", speaking_rate=1.0,
        )

        # 2. Merge caller overrides (only given fields win)
        if settings is not None:
            default_settings.apply_update(settings)

        # 3. Pass to base class
        super().__init__(settings=default_settings, **kwargs)
        self._api_key = api_key
```

### 20.3 What Goes Where?

| Belongs in Settings | Stays as `__init__` params |
|--------------------|-----------------------------|
| Model name, voice, language | API keys, auth tokens |
| Tuning knobs (rate, pitch, temperature) | Base URLs, endpoint overrides |
| Anything changeable mid-session | Audio encoding, sample format |
| | Connection parameters (timeouts, retries) |

### 20.4 Runtime Updates

Services react to `*UpdateSettingsFrame`s (e.g., `TTSUpdateSettingsFrame`). Override `_update_settings`:

```python
async def _update_settings(self, update: TTSSettings) -> dict[str, Any]:
    """Apply a settings update, reconfiguring the connection if needed."""
    changed = await super()._update_settings(update)
    if not changed:
        return changed

    if "language" in changed:
        await self._disconnect()
        await self._connect()

    return changed
```

The returned `dict` maps each changed field name to its **pre-update** value. Use `changed.keys() - {"language"}` for set operations.

### 20.5 Tracing Decorators

Use Pipecat's built-in tracing for observability:

| Decorator | Decorate | Service Type |
|-----------|----------|--------------|
| `@traced_stt` | `_handle_transcription()` | STT |
| `@traced_llm` | `_process_context()` | LLM |
| `@traced_tts` | `run_tts()` | TTS |

---

## 21. Testing Pipecat Code

📄 **Source**: [tests/utils.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/tests/utils.py)

### 21.1 The `run_test()` Utility

Pipecat provides `run_test()` to send frames through a pipeline and assert expected outputs:

```python
from pipecat.tests.utils import run_test
from pipecat.frames.frames import TextFrame, SleepFrame

async def test_my_processor():
    processor = MyCustomProcessor()

    # Define input frames
    frames_to_send = [
        TextFrame(text="hello"),
        SleepFrame(sleep=0.1),       # Add a 100ms delay between frames
        TextFrame(text="world"),
    ]

    # Run test — returns frames that came out in each direction
    (downstream_frames, upstream_frames) = await run_test(
        processor,
        frames_in=frames_to_send,
    )

    # Assert expected output
    assert len(downstream_frames) == 2
    assert downstream_frames[0].text == "HELLO"  # If MyProcessor uppercases
```

### 21.2 Running Tests

```bash
# Run all tests
uv run pytest

# Run a specific test file
uv run pytest tests/test_name.py

# Run a specific test function
uv run pytest tests/test_name.py::test_function_name
```

### 21.3 Test Patterns

- Use `SleepFrame(sleep=N)` to simulate timing between frames
- Test both downstream and upstream frame output
- Test interruption handling by sending `InterruptionFrame` mid-sequence
- Test lifecycle by sending `StartFrame` → frames → `EndFrame`

---

## 22. Contributing to Pipecat

📄 **Source**: [CONTRIBUTING.md](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/CONTRIBUTING.md)

### 22.1 Development Setup

```bash
# Clone and enter the repository
git clone https://github.com/pipecat-ai/pipecat.git
cd pipecat

# Install development dependencies
uv sync --group dev --all-extras --no-extra gstreamer --no-extra local

# Install pre-commit hooks (Ruff linting + formatting)
uv run pre-commit install
```

### 22.2 Changelog Entries

Every user-facing PR needs a changelog fragment in the `changelog/` directory:

```bash
# File naming: <PR_number>.<type>.md
# Types: added, changed, deprecated, removed, fixed, performance, security, other

# Example: changelog/1234.added.md
echo "- Added support for new STT provider." > changelog/1234.added.md

# Preview the changelog
uv run towncrier build --draft --version Unreleased
```

**Skip changelog for**: documentation-only changes, internal refactoring, test-only changes, CI/build changes.

### 22.3 Code Style Rules

| Rule | Detail |
|------|--------|
| **Linter** | Ruff (line length 100) |
| **Docstrings** | Google-style |
| **Type hints** | Required for complex async code |
| **Frames** | Use `@dataclass` (high-frequency, no validation) |
| **Config/Params** | Use Pydantic `BaseModel` (benefits from validation) |
| **Deprecations** | Use `.. deprecated:: <version>` in docstrings + `warnings.warn()` at runtime |

### 22.4 Dependency Management

```bash
# After editing pyproject.toml:
uv lock && uv sync

# Commit both files together:
git add pyproject.toml uv.lock
```

> **Never manually edit `uv.lock`** — it's auto-generated by `uv lock`.

### 22.5 Community Integrations

📄 **Source**: [COMMUNITY_INTEGRATIONS.md](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/COMMUNITY_INTEGRATIONS.md)

Want to add a new service (STT, TTS, LLM, etc.) without modifying the core? Create a community-maintained integration in a separate repository:

1. Name your package `pipecat-{vendor}` (e.g., `pipecat-deepdub`)
2. Extend the appropriate base class (`STTService`, `TTSService`, `LLMService`, etc.)
3. Include a foundational example, README, LICENSE, and changelog
4. Submit a PR to the [docs repository](https://github.com/pipecat-ai/docs) for listing

**Service base class quick reference**:

| Service Type | Base Classes |
|-------------|-------------|
| WebSocket STT | `WebsocketSTTService` |
| SDK-based STT | `STTService` |
| File-based STT | `SegmentedSTTService` |
| OpenAI-compatible LLM | `OpenAILLMService` |
| Custom LLM | `LLMService` + custom adapter |
| WebSocket TTS | `WebsocketTTSService` |
| HTTP TTS | `TTSService` |
| Telephony | `FrameSerializer` |

---

## 23. The Pipecat Ecosystem

Pipecat is more than just the framework — it's an ecosystem of tools and SDKs:

### 23.1 Client SDKs

Connect to Pipecat from any platform:

| SDK | Platform | Link |
|-----|----------|------|
| `@pipecat-ai/client-js` | JavaScript | [Docs](https://docs.pipecat.ai/client/js/introduction) |
| `@pipecat-ai/client-react` | React | [Docs](https://docs.pipecat.ai/client/react/introduction) |
| React Native | Mobile (iOS/Android) | [Docs](https://docs.pipecat.ai/client/react-native/introduction) |
| Swift | iOS | [Docs](https://docs.pipecat.ai/client/ios/introduction) |
| Kotlin | Android | [Docs](https://docs.pipecat.ai/client/android/introduction) |
| C++ | Native/Embedded | [Docs](https://docs.pipecat.ai/client/c++/introduction) |
| ESP32 | IoT/Embedded | [GitHub](https://github.com/pipecat-ai/pipecat-esp32) |

### 23.2 Development Tools

| Tool | Purpose |
|------|---------|
| [Pipecat CLI](https://github.com/pipecat-ai/pipecat-cli) | Create projects, deploy to production |
| [Whisker](https://github.com/pipecat-ai/whisker) | Real-time pipeline debugger |
| [Tail](https://github.com/pipecat-ai/tail) | Terminal dashboard for monitoring |
| [Pipecat Flows](https://github.com/pipecat-ai/pipecat-flows) | Structured conversation state management |
| [Voice UI Kit](https://github.com/pipecat-ai/voice-ui-kit) | Pre-built UI components for voice apps |

### 23.3 Quick Start

```bash
# Create a new project in under a minute
pipecat init quickstart

# Or manually
uv init my-pipecat-app && cd my-pipecat-app
uv add pipecat-ai
uv add "pipecat-ai[daily,openai,deepgram,cartesia]"
```

---

## 24. Deprecations & Migration Guide

As of **Pipecat 1.3.0**, several components have been renamed to align with the multi-worker architecture. Old names still work but emit `DeprecationWarning` and will be removed in a future release.

### 24.1 Naming Changes

| Old Name (Deprecated) | New Name | Module |
|-----------------------|----------|--------|
| `PipelineTask` | `PipelineWorker` | `pipecat.pipeline.worker` |
| `PipelineTaskParams` | `WorkerParams` | `pipecat.pipeline.worker` |
| `PipelineRunner` | `WorkerRunner` | `pipecat.workers.runner` |
| `pipeline_task` (property) | `pipeline_worker` | `FrameProcessor` |
| `tool_resources` | `app_resources` | `PipelineWorker`, `FunctionCallParams` |

### 24.2 Migration Examples

```python
# ❌ Old (deprecated, still works but warns)
from pipecat.pipeline.task import PipelineTask
from pipecat.pipeline.runner import PipelineRunner

task = PipelineTask(pipeline, params=PipelineTaskParams(...))
runner = PipelineRunner()
await runner.run(task)  # Passing worker to run() is deprecated

# ✅ New (recommended)
from pipecat.pipeline.worker import PipelineWorker
from pipecat.workers.runner import WorkerRunner

worker = PipelineWorker(pipeline, params=WorkerParams(...))
runner = WorkerRunner()
await runner.add_workers(worker)  # Register first
await runner.run()                 # Then run
```

### 24.3 Other Deprecations

| Deprecated | Replacement | Since |
|-----------|-------------|-------|
| `ResampyResampler` | `SOXRAudioResampler` | 1.2.0 |
| `filter_incomplete_user_turns` (param) | `FilterIncompleteUserTurnStrategies` | 1.2.0 |
| `AICVADAnalyzer` | `AICQuailVADAnalyzer` | 1.3.0+ |
| `transformers` (base dependency) | Only via `local-smart-turn` / `moondream` extras | 1.3.0 |

---

## 25. Putting It All Together

Here's a complete voice bot showing how every component connects:

```python
import asyncio
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineWorker
from pipecat.workers.runner import WorkerRunner
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.transports.network.daily.transport import DailyTransport, DailyParams
from pipecat.processors.aggregators.llm_context import LLMContext

async def main():
    # 1. TRANSPORT — How audio enters/exits
    transport = DailyTransport(
        room_url="https://your-domain.daily.co/room",
        token="...",
        bot_name="MyBot",
        params=DailyParams(audio_in_enabled=True, audio_out_enabled=True),
    )

    # 2. SERVICES — AI providers
    stt = DeepgramSTTService(api_key="...")       # Speech → Text
    llm = OpenAILLMService(model="gpt-4o")        # Text → Text (AI)
    tts = CartesiaTTSService(api_key="...")        # Text → Speech

    # 3. CONTEXT — Conversation memory
    context = LLMContext(messages=[
        {"role": "system", "content": "You are a helpful voice assistant."},
    ])
    context_aggregator = llm.create_context_aggregator(context)

    # 4. PIPELINE — Wire everything together
    pipeline = Pipeline([
        transport.input(),                         # Audio from user
        stt,                                       # Audio → Text
        context_aggregator.user(),                 # Collect user text
        llm,                                       # Generate response
        tts,                                       # Response → Audio
        transport.output(),                        # Audio to user
        context_aggregator.assistant(),            # Collect bot text
    ])

    # 5. WORKER + RUNNER — Execute
    worker = PipelineWorker(pipeline)
    runner = WorkerRunner()
    await runner.add_workers(worker)
    await runner.run()                             # Blocks until done

asyncio.run(main())
```

### The Data Flow for "Hello, how are you?"

```
1. User speaks "Hello, how are you?"
   └─▶ DailyTransport.input() produces InputAudioRawFrame chunks

2. STT receives audio chunks
   └─▶ DeepgramSTT sends to API, receives TranscriptionFrame("Hello, how are you?")

3. User Context Aggregator receives TranscriptionFrame
   └─▶ Waits for UserStoppedSpeakingFrame, then:
       - Adds {"role": "user", "content": "Hello, how are you?"} to LLMContext
       - Pushes LLMContextFrame with full context

4. LLM receives LLMContextFrame
   └─▶ Streams response token by token:
       TextFrame("I'm") → TextFrame(" doing") → TextFrame(" great") → TextFrame("!")

5. TTS receives TextFrames
   └─▶ Aggregates into sentences, sends to API
       Returns TTSAudioRawFrame chunks with synthesized audio

6. DailyTransport.output() receives audio
   └─▶ Chunks into 40ms segments, sends via WebRTC to user's browser

7. Assistant Context Aggregator collects all TextFrames
   └─▶ Adds {"role": "assistant", "content": "I'm doing great!"} to LLMContext
```

---

## 26. Quick Reference: Key Patterns

### 🔑 Rules You Must Follow

| Rule | Why |
|------|-----|
| Always call `super().process_frame()` first | Ensures system frame handling and metrics work correctly |
| Always push frames you don't handle | Swallowing frames breaks downstream processors |
| Push in the direction the frame came from | Unless you're intentionally redirecting (e.g., errors upstream) |
| Use `self.create_task()` not `asyncio.create_task()` | TaskManager tracks and cancels tasks on shutdown |
| Use `broadcast_interruption()` to trigger interruptions | Replaces the deprecated `push_interruption_task_frame_and_wait()` |
| Use `@dataclass` for frames, `BaseModel` for config | Frames are high-frequency (no validation overhead needed) |
| Push errors with `await self.push_error(msg, exc)` | Lets upstream handle errors; use `fatal=False` by default |
| Access worker via `self.pipeline_worker` | Not `self.pipeline_task` (deprecated since 1.3.0) |

### 🏗️ Common Patterns

**Creating a context aggregator pair**:
```python
context = LLMContext(messages=[{"role": "system", "content": "..."}])
aggregator = llm.create_context_aggregator(context)
pipeline = Pipeline([..., aggregator.user(), llm, tts, ..., aggregator.assistant()])
```

**Registering a function for the LLM to call**:
```python
async def get_weather(params: FunctionCallParams):
    city = params.arguments.get("city")
    return {"temperature": 72, "city": city}

llm.register_function("get_weather", get_weather)
```

**Adding event handlers**:
```python
@transport.event_handler("on_participant_joined")
async def on_joined(transport, participant):
    print(f"{participant['info']['userName']} joined!")
```

**Using observers for monitoring**:
```python
worker = PipelineWorker(pipeline, observers=[UserBotLatencyObserver()])
```

**Multi-worker handoff**:
```python
runner = WorkerRunner()
await runner.add_workers(greeter_worker, support_worker, pipeline_worker)
await runner.run()
```

**Realtime service mode (S2S)**:
```python
aggregator = llm.create_context_aggregator(context, realtime_service_mode=True)

@aggregator.user().event_handler("on_user_turn_message_added")
async def on_msg(agg, message):
    print(f"User said: {message}")
```

**Runtime settings update**:
```python
# Send an update frame to change TTS voice mid-conversation
from pipecat.frames.frames import TTSUpdateSettingsFrame
await processor.push_frame(TTSUpdateSettingsFrame(
    settings=tts.Settings(voice="new-voice-id")
))
```

### 📊 LLM Response Frame Sequence

Every LLM response **must** follow this frame sequence:

```
LLMFullResponseStartFrame        ← Signals response start
├── LLMTextFrame("Hello")        ← Streamed token 1
├── LLMTextFrame(" there")       ← Streamed token 2
├── LLMTextFrame("!")             ← Streamed token N
LLMFullResponseEndFrame          ← Signals response end
```

For **reasoning models** (chain-of-thought), thought frames are interleaved:

```
LLMThoughtStartFrame             ← Thinking begins
├── LLMThoughtTextFrame("Let me consider...")
├── LLMThoughtTextFrame("The user wants...")
LLMThoughtEndFrame               ← Thinking ends
LLMFullResponseStartFrame        ← Response begins
├── LLMTextFrame("Here's my answer...")
LLMFullResponseEndFrame          ← Response ends
```

### 📁 Key Files to Read First

1. [frames.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/frames/frames.py) — Understand the data model
2. [frame_processor.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/processors/frame_processor.py) — Understand the processing engine
3. [pipeline.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/pipeline/pipeline.py) — Understand how processors chain together
4. [worker.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/pipeline/worker.py) — Understand the runtime wrapper
5. [runner.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/workers/runner.py) — Understand the entry point
6. [llm_service.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/services/llm_service.py) — Understand LLM integration patterns
7. [llm_context.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/processors/aggregators/llm_context.py) — Understand conversation memory
8. [base_worker.py](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/src/pipecat/workers/base_worker.py) — Understand multi-worker architecture

### 🐛 Common Pitfalls

| Pitfall | Fix |
|---------|-----|
| Pipeline hangs on shutdown | Check for processors that don't forward EndFrame |
| Audio not playing | Ensure `audio_out_enabled=True` in TransportParams |
| LLM not responding | Check that context aggregator pair is correctly placed |
| Interruptions not working | Ensure VAD is configured and turn start strategy is active |
| Tasks leaking after shutdown | Use `self.create_task()` instead of `asyncio.create_task()` |
| Frames disappearing | A processor is swallowing frames instead of pushing them through |
| `AttributeError` on `pipeline_task` | Use `pipeline_worker` instead (deprecated in 1.3.0) |
| S2S service missing user messages | Use `realtime_service_mode=True` on the context aggregator pair |
| LLM hallucinating removed tools | Enable `add_tool_change_messages=True` on `LLMContextAggregatorPair` |
| TTS deadlock on interruption | Ensure your service handles `pause_frame_processing` correctly |

### 🗺️ Where to Go Next

| Goal | Resource |
|------|----------|
| Build your first voice bot | [Getting Started Examples](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/examples/getting-started) |
| Build a multi-agent system | [Multi-Worker Examples](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/examples/multi-worker) |
| Add a new AI service | [COMMUNITY_INTEGRATIONS.md](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/COMMUNITY_INTEGRATIONS.md) |
| Contribute to the framework | [CONTRIBUTING.md](file:///media/nikki/Data/Projects/OpenSource/pipecat-ai/pipecat/CONTRIBUTING.md) |
| Debug a pipeline | [Whisker](https://github.com/pipecat-ai/whisker) or [Tail](https://github.com/pipecat-ai/tail) |
| Read the official docs | [docs.pipecat.ai](https://docs.pipecat.ai) |
| Get help | [Discord](https://discord.gg/pipecat) |

---

> 📝 **This document was created for the `learn` branch as a comprehensive educational reference. It covers Pipecat as of version 1.3.0. For the latest API docs, always check the source code and [docs.pipecat.ai](https://docs.pipecat.ai).**

