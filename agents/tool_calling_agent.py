import json
from typing import Any, List, Union

from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

from agents.agent_interface import AgentInterface, AgentResponse, Message
from agents.components.result_handle_manager import ResultHandleManager

class ToolCallingAgent(AgentInterface):
    """Agent that executes tool-calling loops with handle-based result management"""

    def __init__(
        self,
        llm: Any,
        tools: list,
        initial_data_handle: str,
        max_iterations: int = 10,
        **kwargs,
    ):
        """Initialize agent with LLM and tools via dependency injection

        Args:
            llm_with_tools: LLM instance with tools already bound via llm.bind_tools()
            mcp_tools: List of MCP tool instances
            initial_data_handle: Handle string for initial dataset
            max_iterations: Maximum number of agent loop iterations (default: 10)
        """
        self.handle_manager = ResultHandleManager()
        self._llm_with_tools = llm.bind_tools(tools)
        self._mcp_tools = tools
        self._initial_data_handle = initial_data_handle
        self._max_iterations = max_iterations

    def _build_system_message(self) -> str:
        """Build system message explaining handle system"""
        # Get initial data reference with key_names and preview
        initial_data_info = ""
        try:
            initial_data = self.handle_manager.get_result(self._initial_data_handle)
            initial_data_reference = self.handle_manager.create_reference(
                self._initial_data_handle,
                initial_data
            )
            # Parse the reference to extract key_names and preview
            ref_dict = json.loads(initial_data_reference)
            if ref_dict.get("type") == "data_table":
                key_names = ref_dict.get("key_names", [])
                preview = ref_dict.get("preview", {})
                initial_data_info = f"""
- Available columns (key_names): {key_names}
- Preview of first few rows: {json.dumps(preview, indent=2)}"""
        except (ValueError, KeyError, json.JSONDecodeError):
            # If we can't get the data, just show the handle
            pass

        return f"""
You are a helpful data analysis assistant. Use the available tools and data to answer queries.
Think step by step and provide reasoning for the action that you are planning to take.

IMPORTANT - Tool Result Handles:
- Tool results are returned as handles/references, not full data
- Data tables have: {{"handle": "filtered_data_1", "type": "data_table", "key_names": [...], "preview": {{...}}}}
- Remember that the "preview" is only the first few values of the full data_table.
- To use a result in another tool, pass the handle string as the 'data' argument
- Example: After filter_data returns {{"handle": "filtered_superhero_1", ...}}
  Call: sort_data(data="filtered_superhero_1", key_name="age", ...)
- Scalar results include both handle and value: {{"handle": "count_1", "type": "scalar", "value": 42}}
- Always use handles to reference previous results in tool chains
- Make at most a single tool call per iteration. Once you have the information necessary to answer the query, return the answer with no additional tool calls.
- If you receive an error from the tool, reason over why your previous action resulted in that error and make the appropriate fix. 

INITIAL DATA:
- The initial dataset for this task is available as handle: "{self._initial_data_handle}"{initial_data_info}
- Start by using this handle in your first tool call
        """

    async def run(
        self,
        input: Union[str, List[Message]],
    ) -> AgentResponse:
        """Execute agent loop and return AgentResponse.

        Args:
            input: Either a plain query string or a list of Messages
                representing a partial conversation.  When a string is
                given the system message is prepended automatically.
                When a list is given the system message is prepended
                unless the list already contains a system message.

        Returns:
            AgentResponse with content, tool_calls, and trajectory
        """
        system_prompt = self._build_system_message()

        if isinstance(input, str):
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=input),
            ]
            query_preview = input
        else:
            messages = []
            has_system = any(m.role == "system" for m in input)
            if not has_system:
                messages.append(SystemMessage(content=system_prompt))
            for m in input:
                if m.role == "user":
                    messages.append(HumanMessage(content=m.content))
                elif m.role == "assistant":
                    messages.append(AIMessage(content=m.content))
                elif m.role == "system":
                    messages.append(SystemMessage(content=m.content))
            query_preview = next(
                (m.content for m in reversed(input) if m.role == "user"),
                "",
            )

        print(f"\n{'='*80}")
        print(f"Query: {query_preview}")
        print(f"{'='*80}\n")

        print("SYSTEM:")
        print(system_prompt)

        # Run agent loop
        for iteration in range(self._max_iterations):
            print(f"\n{'='*80}")
            print(f"ITERATION {iteration + 1}/{self._max_iterations}")
            print(f"{'='*80}")
            print(f"Message history length: {len(messages)}")

            # Invoke LLM
            print("Invoking LLM...")
            response = await self._llm_with_tools.ainvoke(messages)
            print("LLM response received")

            if response.content:
                content_preview = str(response.content)[:200]
                print(
                    f"Response content (first 200 chars): {content_preview}"
                )

            # Add AI response to messages
            messages.append(response)

            # Check if done (no tool calls)
            if not response.tool_calls:
                print("\nNo tool calls - Agent is done!")
                print(f"Final Answer: {response.content}")
                return self._build_response(response.content, messages)

            # Execute tool calls
            print(f"\nNumber of tool calls: {len(response.tool_calls)}")
            for idx, tool_call in enumerate(response.tool_calls, 1):
                print(f"\n--- Tool Call {idx}/{len(response.tool_calls)} ---")
                await self._execute_tool_call(tool_call, messages)

        # Max iterations reached
        print(f"\nReached max iterations ({self._max_iterations})")
        return self._build_response(
            f"Max iterations reached ({self._max_iterations})", messages
        )

    async def _execute_tool_call(self, tool_call: dict, messages: list):
        """Execute a single tool call and append result to messages.

        Args:
            tool_call: Dict with 'name', 'args', and 'id' keys
            messages: Running message list for this invocation
        """
        tool_name = tool_call['name']
        tool_args = tool_call['args']
        tool_id = tool_call['id']

        print("\n  === Executing Tool Call ===")
        print(f"  Tool: {tool_name}")
        print(f"  Tool ID: {tool_id}")

        # Unwrap kwargs if present (fixes LLM schema interpretation issue)
        if len(tool_args) == 1 and 'kwargs' in tool_args:
            print("  Unwrapping kwargs...")
            tool_args = tool_args['kwargs']

        print("  Original Arguments:")
        for k, v in tool_args.items():
            print(f"    {k}: {v}")

        # Find matching tool
        matching_tool = next((t for t in self._mcp_tools if t.name == tool_name), None)

        if not matching_tool:
            print(f"  ERROR: Tool '{tool_name}' not found in available tools!")
            error_msg = json.dumps({"error": f"Tool {tool_name} not found"})
            messages.append(ToolMessage(
                content=error_msg,
                tool_call_id=tool_id,
                name=tool_name
            ))
            return

        try:
            # Resolve any handle references in arguments
            print("  Resolving handle references...")
            resolved_args = self.handle_manager.resolve_args(tool_args, resolve_data=True)
            # print("  Resolved Arguments:")
            # for k, v in resolved_args.items():
            #     v_str = str(v)
            #     if len(v_str) > 200:
            #         v_str = v_str[:200] + "..."
            #     print(f"    {k}: {v_str}")

            # Execute tool with resolved arguments
            print("  Invoking tool...")
            result = await matching_tool.ainvoke(resolved_args)
            # print(f"  Raw result length: {len(result) if result else 0}")
            # print(f"  Raw result (first 500 chars): {str(result)[:500]}")

            parsed_result = json.loads(result)
            print(f"  Parsed result type: {type(parsed_result)}")

            # Extract text field from MCP TextContent format
            if isinstance(parsed_result, dict) and 'type' in parsed_result and 'text' in parsed_result:
                print("  Extracting text from MCP TextContent format...")
                parsed_result = parsed_result['text']

                # If the text field contains JSON, parse it
                if isinstance(parsed_result, str):
                    try:
                        parsed_result = json.loads(parsed_result)
                        print("  Successfully parsed JSON from text field")
                    except (json.JSONDecodeError, ValueError):
                        # Not JSON, keep as string
                        print("  Text field is not JSON, keeping as string")
                        pass

            print(f"  Final parsed result type: {type(parsed_result)}")
            result_str = str(parsed_result)
            if len(result_str) > 300:
                result_str = result_str[:300] + "..."
            print(f"  Final parsed result: {result_str}")

            # Check if result contains an error
            if isinstance(parsed_result, dict) and "error" in parsed_result:
                # Don't store errors - return them directly in the message
                print(f"  ERROR DETECTED in result: {parsed_result['error']}")
                error_msg = json.dumps(parsed_result)
                messages.append(ToolMessage(
                    content=error_msg,
                    tool_call_id=tool_id,
                    name=tool_name
                ))
                print("  Error message sent to LLM")
                return
            elif isinstance(parsed_result, str) and "error" in parsed_result.lower():
                # Handle plain string error messages
                print(f"  ERROR DETECTED in string result: {parsed_result}")
                error_msg = json.dumps({"error": parsed_result})
                messages.append(ToolMessage(
                    content=error_msg,
                    tool_call_id=tool_id,
                    name=tool_name
                ))
                print("  Error message sent to LLM")
                return

            # Store parsed result and generate handle
            print("  Storing result in handle manager...")
            handle = self.handle_manager.store_result(tool_name, parsed_result)
            print(f"  Result stored with handle: {handle}")

            # Create reference for message
            print("  Creating reference for LLM...")
            reference = self.handle_manager.create_reference(handle, parsed_result)
            print(f"  Reference created (length: {len(reference)})")
            # print(f"  Reference content (first 300 chars): {reference[:300]}")

            # Add tool result with reference instead of full data
            messages.append(ToolMessage(
                content=reference,
                tool_call_id=tool_id,
                name=tool_name
            ))
            print("  SUCCESS: Tool execution complete\n")

        except Exception as e:
            print(f"  EXCEPTION CAUGHT: {type(e).__name__}")
            print(f"  Exception message: {str(e)}")
            import traceback
            print("  Traceback:")
            traceback.print_exc()
            error_msg = json.dumps({"error": str(e)})
            messages.append(ToolMessage(
                content=error_msg,
                tool_call_id=tool_id,
                name=tool_name
            ))
            print("  Error message sent to LLM\n")

    def _build_response(self, content: str, messages: list) -> AgentResponse:
        """Build AgentResponse from message history."""
        tool_calls = []
        trajectory = []
        tool_call_args = {}

        for msg in messages:
            msg_class = msg.__class__.__name__
            trajectory_entry = {
                "type": msg_class,
                "content": getattr(msg, "content", ""),
            }

            if msg_class == "HumanMessage":
                trajectory.append(trajectory_entry)

            elif msg_class == "AIMessage":
                if hasattr(msg, "tool_calls") and msg.tool_calls:
                    trajectory_entry["tool_calls"] = []
                    for tc in msg.tool_calls:
                        tc_id = tc.get("id", "") or tc.get("tool_call_id", "")
                        tool_call_args[tc_id] = {
                            "name": tc.get("name", "unknown"),
                            "args": tc.get("args", {}),
                        }
                        trajectory_entry["tool_calls"].append({
                            "id": tc_id,
                            "name": tc.get("name", "unknown"),
                            "args": tc.get("args", {}),
                        })
                trajectory.append(trajectory_entry)

            elif msg_class == "ToolMessage":
                tool_call_id = getattr(msg, "tool_call_id", "")
                tool_info = tool_call_args.get(tool_call_id, {})
                tool_name = (
                    getattr(msg, "name", None) or tool_info.get("name", "unknown")
                )
                tool_calls.append({
                    "tool_name": tool_name,
                    "arguments": tool_info.get("args", {}),
                    "result": msg.content,
                })
                trajectory_entry["tool_name"] = tool_name
                trajectory_entry["tool_call_id"] = tool_call_id
                trajectory_entry["result"] = msg.content
                trajectory.append(trajectory_entry)

            elif msg_class == "SystemMessage":
                trajectory.append(trajectory_entry)

        return AgentResponse(
            content=content,
            tool_calls=tool_calls,
            messages=[],
            metadata={},
            trajectory=trajectory,
        )

    def restart(self):
        """Clear handle manager state between queries."""
        self.handle_manager.clear()
