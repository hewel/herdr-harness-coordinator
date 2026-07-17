# Herdr Agent Orchestrator

Herdr Agent Orchestrator is the control plane that coordinates bounded coding-agent work while preserving one authority for workflow, policy, repository safety, and results.

## Control plane

**Orchestrator**:
The single top-level authority that owns workflows and coordinates agent runs.
_Avoid_: Coordinator, provider orchestrator

**Parent Agent**:
The agent responsible for user intent, architecture decisions, task decomposition, acceptance criteria, final review, and the user-facing response.
_Avoid_: Root worker, supervisor

**Provider**:
An execution engine that runs an assigned task without owning the top-level workflow.
_Avoid_: Role, agent type

**Role**:
The responsibility assigned to an agent run, independent of which provider executes it.
_Avoid_: Provider profile, agent type

**Execution Policy**:
The enforceable permissions and limits that bound an agent run.
_Avoid_: Prompt instructions, role description

## Work

**Task Packet**:
A bounded, resolved objective with its context, requirements, acceptance criteria, write scope, and verification expectations.
_Avoid_: Prompt, ticket

**Agent Run**:
One provider session executing one task packet under a resolved role and execution policy.
_Avoid_: Agent, workflow

**Workflow Run**:
One orchestrator-owned execution of a workflow template that coordinates child Agent Runs and produces one terminal workflow outcome.
_Avoid_: Agent Run, provider session

**Workflow Node**:
A dependency-aware unit of workflow execution that produces an artifact for downstream nodes.
_Avoid_: Child agent, task packet

**Delegation Mode**:
The ownership model governing how an agent run may use provider-native child execution.
_Avoid_: Provider mode, role mode

**Managed Mode**:
A delegation mode in which the orchestrator creates and controls every workflow node.
_Avoid_: Default provider mode

**Native Mode**:
A delegation mode in which a provider uses its own multi-agent system within a run.
_Avoid_: Managed mode

**Hybrid Mode**:
A delegation mode in which the orchestrator owns the top-level workflow while a provider run may create controlled native children.
_Avoid_: Mixed workflow ownership

## Exchange and safety

**Structured Artifact**:
A versioned result produced by an agent run and consumed without depending on provider-native conversation history.
_Avoid_: Final message, transcript

**Handoff Packet**:
A structured artifact that transfers bounded context and instructions between agent runs.
_Avoid_: Shared chat history

**Repository Guard**:
The authority that validates repository baselines, write scopes, edit exclusivity, and final changes for an agent run.
_Avoid_: Git wrapper, prompt rule
