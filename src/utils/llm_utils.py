import os

# Neither ChatOpenAI nor ChatMistralAI had a bound timeout, so a hung model
# could keep the API thread alive indefinitely (0.8). max_retries=0 hands
# retry ownership entirely to `tenacity` (see resilience.retry_transient) so
# the two don't compound into surprising multi-minute retry storms.
LLM_TIMEOUT_SECONDS = float(os.environ.get("LLM_TIMEOUT_SECONDS", "60"))

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
            from langchain_openai import ChatOpenAI
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
