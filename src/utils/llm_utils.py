import os

# Neither ChatOpenAI nor ChatMistralAI had a bound timeout, so a hung model
# could keep the API thread alive indefinitely (0.8). max_retries=0 hands
# retry ownership entirely to `tenacity` (see resilience.retry_transient) so
# the two don't compound into surprising multi-minute retry storms.
LLM_TIMEOUT_SECONDS = float(os.environ.get("LLM_TIMEOUT_SECONDS", "60"))

_reasoning_chat_openai = None


def _chat_openai_class():
    """ChatOpenAI, keeping a reasoning model's thinking.

    vLLM serves a reasoning model (glm-5.2) with its thinking in the reply's
    `reasoning_content` field, beside the answer. ChatOpenAI targets the
    official OpenAI API and drops fields it does not know, so the thinking --
    usually most of a call's output tokens -- reached neither the trace nor
    anything else: a trace showed what an agent decided and never why. This
    keeps it in the AIMessage's `additional_kwargs["reasoning_content"]`
    (`reasoning`, as some servers name it, is read too). The answer itself is
    untouched: `content` is what it was, and what is sent back to the model
    on the next turn leaves additional_kwargs out, so the model never sees its
    old thinking again. Built on first use, like the import it needs.
    """
    global _reasoning_chat_openai
    if _reasoning_chat_openai is None:
        from langchain_openai import ChatOpenAI

        class ReasoningChatOpenAI(ChatOpenAI):
            def _create_chat_result(self, response, generation_info=None):
                result = super()._create_chat_result(response, generation_info)
                try:
                    dumped = response if isinstance(response, dict) else response.model_dump(
                        exclude={"choices": {"__all__": {"message": {"parsed"}}}})
                    for choice, generation in zip(dumped.get("choices") or [],
                                                  result.generations):
                        message = choice.get("message") or {}
                        reasoning = message.get("reasoning_content") or message.get("reasoning")
                        if isinstance(reasoning, str) and reasoning.strip():
                            generation.message.additional_kwargs["reasoning_content"] = reasoning
                except Exception:
                    # The answer stands without its reasoning.
                    pass
                return result

        _reasoning_chat_openai = ReasoningChatOpenAI
    return _reasoning_chat_openai


# The only two tiers any caller passes (see agent_orchestrator.get_agent).
# A typo'd or stale tier name (e.g. "medium") used to silently fall through
# to the "simple" branch via the bare `else` below (1.4) -- fail fast instead.
_VALID_TIERS = ("complex", "simple")

def get_llm(tier: str = "complex"):
    """
    Centralized LLM factory.
    If USE_HF=true, uses Hugging Face Inference Endpoints as a free fallback.
    Otherwise, uses the local/proxy OpenAI-compatible endpoint.
    """
    if tier not in _VALID_TIERS:
        raise ValueError(f"Unknown LLM tier '{tier}'. Valid tiers: {_VALID_TIERS}")

    use_hf = os.environ.get("USE_HF", "false").lower() == "true"

    if use_hf:
        try:
            from langchain_huggingface import ChatHuggingFace, HuggingFaceEndpoint
        except ImportError:
            raise ImportError("Please install langchain-huggingface to use the HF fallback: pip install langchain-huggingface")
            
        if tier == "complex":
            model_id = os.environ.get("HF_MODEL_COMPLEX", "THUDM/glm-4-9b-chat")
        else:
            model_id = os.environ.get("HF_MODEL_SIMPLE", "Qwen/Qwen3-VL-235B-A22B-Instruct-FP8")
            
        hf_token = os.environ.get("HF_TOKEN")
        if not hf_token:
            raise ValueError("HF_TOKEN environment variable must be set when USE_HF=true")
            
        llm = HuggingFaceEndpoint(
            repo_id=model_id,
            task="text-generation",
            huggingfacehub_api_token=hf_token,
            temperature=0.1,
            max_new_tokens=4096
        )
        return ChatHuggingFace(llm=llm)
    else:
        use_mistral = os.environ.get("MOCK_LLM_WITH_MISTRAL", "false").lower() == "true"
        
        if use_mistral:
            try:
                from langchain_mistralai import ChatMistralAI
            except ImportError:
                raise ImportError("Please install langchain-mistralai to use Mistral: pip install langchain-mistralai")
                
            if tier == "complex":
                model = os.environ.get("MISTRAL_MODEL_COMPLEX", "mistral-large-latest")
                api_key = os.environ.get("LLM_API_KEY_COMPLEX", "dummy")
            else:
                model = os.environ.get("MISTRAL_MODEL_SIMPLE", "mistral-small-latest")
                api_key = os.environ.get("LLM_API_KEY_SIMPLE", "dummy")
                
            return ChatMistralAI(
                model=model,
                mistral_api_key=api_key,
                temperature=0,
                timeout=LLM_TIMEOUT_SECONDS,
                max_retries=0
            )
        else:
            ChatOpenAI = _chat_openai_class()
            if tier == "complex":
                model = os.environ.get("LLM_MODEL_COMPLEX", "glm-5.2-fp8")
                base_url = os.environ.get("LLM_BASE_URL_COMPLEX", "http://localhost:8000/v1")
                api_key = os.environ.get("LLM_API_KEY_COMPLEX", "dummy")
            else:
                model = os.environ.get("LLM_MODEL_SIMPLE", "Qwen/Qwen3-VL-235B-A22B-Instruct-FP8")
                base_url = os.environ.get("LLM_BASE_URL_SIMPLE", "http://localhost:8000/v1")
                api_key = os.environ.get("LLM_API_KEY_SIMPLE", "dummy")
                
            return ChatOpenAI(
                model=model,
                base_url=base_url,
                api_key=api_key,
                temperature=0,
                timeout=LLM_TIMEOUT_SECONDS,
                max_retries=0
            )
