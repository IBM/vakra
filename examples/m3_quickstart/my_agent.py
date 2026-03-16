"""
my_agent.py — Example agent to plug into run_benchmark.py.

Replace this file (or just the `run_query` function) with your own agent.

Contract:
    async def run_query(session, query, provider, model) -> str

    Args:
        session  - An initialised mcp.ClientSession connected to the capability server.
                   Call `await session.call_tool(name, args)` to invoke tools.
        query    - The natural-language question the agent must answer.
        provider - LLM provider name: "anthropic", "openai", or "ollama".
        model    - Model identifier, e.g. "claude-sonnet-4-6" or None for default.

    Returns:
        The agent's final text answer as a string.
"""
import os
import sys

# Make project root importable when running from any directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from agents.llm import create_llm
from agents.agent_interface import LangGraphReActAgent
from agents.mcp_tool_wrapper import MCPToolWrapper


async def run_query(session, query: str, provider: str = "anthropic", model: str = None) -> str:
    """Run a single query using a LangGraph ReAct agent.

    Fetches available tools from the MCP session, creates a ReAct agent,
    and runs the query to completion.

    Replace this function with your own agent implementation.
    The only contract is: accept (session, query, provider, model) and return a string.
    """
    # Convert MCP tools to LangChain StructuredTools
    wrapper = MCPToolWrapper(session)
    tools = await wrapper.get_tools()

    # Create an LLM from the chosen provider
    llm = create_llm(provider=provider, model=model)

    # Create and run a ReAct agent
    agent = LangGraphReActAgent(llm=llm, tools=tools)
    response = await agent.run(query)
    return response.content
