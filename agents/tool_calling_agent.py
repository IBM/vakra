"""
Deprecated: ToolCallingAgent is superseded by LangGraphReActAgent with
use_handle_manager=True. This module is kept for backward compatibility.
"""
import warnings

from agents.agent_interface import LangGraphReActAgent


class ToolCallingAgent(LangGraphReActAgent):
    """Deprecated alias for LangGraphReActAgent(use_handle_manager=True).

    Use LangGraphReActAgent directly with use_handle_manager=True instead.
    """

    def __init__(self, llm, tools, initial_data_handle, max_iterations=10, **kwargs):
        warnings.warn(
            "ToolCallingAgent is deprecated. Use LangGraphReActAgent with "
            "use_handle_manager=True instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(
            llm=llm,
            tools=tools,
            use_handle_manager=True,
            initial_data_handle=initial_data_handle,
            max_iterations=max_iterations,
            **kwargs,
        )
