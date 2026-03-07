# =============================================================================
# 02_extract.py
# Runs complaint text through one or more LLMs and saves JSON outputs to
# data/extracted/<model>/
#
# Reads:  data/filtered_texts.csv + prompt.txt
# Writes: data/extracted/<llm_type>/<file_id>_<model>_<date>.txt (per file)
#         data/extracted/<llm_type>/summary_<date>.json
#         data/combined_summary_<date>.json
# =============================================================================

import os
import json
import time
import asyncio
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional
import config

from openai import AsyncOpenAI
from anthropic import AsyncAnthropic
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold

# ----------------------------- PATHS -----------------------------------------

PROMPT_FILE   = "prompt.txt"
INPUT_CSV     = "data/filtered_texts.csv"
BASE_OUT_DIR  = "data/extracted/"

# ----------------------------- PARAMETERS ------------------------------------

BATCH_SIZE   = 15    # Max concurrent requests per model
BATCH_DELAY  = 0.1
SAMPLE_SIZE  = 500   # Set to None to process all rows

# ----------------------------- API KEYS --------------------------------------

OPENAI_API_KEY      = config.OPENAI_API_KEY
ANTHROPIC_API_KEY   = config.ANTHROPIC_API_KEY
GOOGLE_API_KEY      = config.GOOGLE_API_KEY
HUGGINGFACE_API_KEY = config.HUGGINGFACE_API_KEY

# ----------------------------- MODEL REGISTRY --------------------------------

MODELS = {
    "openai": {
        "model_name":  "gpt-4o-mini",
        "enabled":     True,
        "client_type": "openai",
        "max_tokens":  16384,
    },
    "claude": {
        "model_name":  "claude-3-5-sonnet-20241022",
        "enabled":     False,
        "client_type": "anthropic",
        "max_tokens":  16384,
    },
    "gemini": {
        "model_name":  "gemini-2.5-flash-lite",
        "enabled":     False,
        "client_type": "google",
        "max_tokens":  16384,
    },
    "llama": {
        "model_name":  "meta-llama/Llama-3.3-70B-Instruct",
        "enabled":     False,
        "client_type": "llama",
        "max_tokens":  16384,
    },
    "deepseek": {
        "model_name":  "deepseek-ai/DeepSeek-V3.2:novita",
        "enabled":     False,
        "client_type": "deepseek",
        "max_tokens":  16384,
    },
}

SYSTEM_PROMPT = "You are a legal data extraction system. Respond ONLY with valid JSON."

# ----------------------------- CLIENT BASE -----------------------------------

class LLMClient:
    def __init__(self, model_name: str, llm_type: str):
        self.model_name = model_name
        self.llm_type   = llm_type
        self.output_dir = os.path.join(BASE_OUT_DIR, llm_type)
        os.makedirs(self.output_dir, exist_ok=True)

    def get_existing_file_ids(self) -> set:
        ids = set()
        for fname in os.listdir(self.output_dir):
            if fname.endswith(".txt"):
                ids.add(fname.split("_")[0])
        return ids

    async def process(self, prompt: str) -> Dict[str, Any]:
        raise NotImplementedError

# ----------------------------- CLIENT IMPLEMENTATIONS ------------------------

class OpenAIClient(LLMClient):
    def __init__(self, model_name: str, max_tokens: int = 16384):
        super().__init__(model_name, "openai")
        self.client     = AsyncOpenAI(api_key=OPENAI_API_KEY)
        self.max_tokens = max_tokens

    async def process(self, prompt: str) -> Dict[str, Any]:
        r = await self.client.chat.completions.create(
            model=self.model_name,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": prompt},
            ],
            temperature=0,
            max_tokens=self.max_tokens,
        )
        return {
            "content": r.choices[0].message.content,
            "tokens":  r.usage.total_tokens if hasattr(r, "usage") else None,
        }


class ClaudeClient(LLMClient):
    def __init__(self, model_name: str, max_tokens: int = 16384):
        super().__init__(model_name, "claude")
        self.client     = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
        self.max_tokens = max_tokens

    async def process(self, prompt: str) -> Dict[str, Any]:
        r = await self.client.messages.create(
            model=self.model_name,
            max_tokens=self.max_tokens,
            temperature=0,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return {
            "content": r.content[0].text,
            "tokens":  r.usage.input_tokens + r.usage.output_tokens,
        }


class GeminiClient(LLMClient):
    def __init__(self, model_name: str, max_tokens: int = 16384):
        super().__init__(model_name, "gemini")
        genai.configure(api_key=GOOGLE_API_KEY)
        safety = {
            HarmCategory.HARM_CATEGORY_HARASSMENT:        HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HATE_SPEECH:       HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
        }
        self.model = genai.GenerativeModel(
            model_name=model_name,
            safety_settings=safety,
            generation_config={"temperature": 0, "top_p": 1, "top_k": 1, "max_output_tokens": max_tokens},
        )

    async def process(self, prompt: str) -> Dict[str, Any]:
        loop = asyncio.get_event_loop()
        full_prompt = f"{SYSTEM_PROMPT}\n\n{prompt}"
        r = await loop.run_in_executor(None, lambda: self.model.generate_content(full_prompt))
        tokens = None
        if hasattr(r, "usage_metadata"):
            tokens = r.usage_metadata.prompt_token_count + r.usage_metadata.candidates_token_count
        return {"content": r.text, "tokens": tokens}


class _OpenAICompatClient(LLMClient):
    """Shared base for HuggingFace-routed models (LLaMA, DeepSeek)."""
    def __init__(self, model_name: str, llm_type: str, max_tokens: int = 16384):
        super().__init__(model_name, llm_type)
        self.client     = AsyncOpenAI(api_key=HUGGINGFACE_API_KEY, base_url="https://router.huggingface.co/v1")
        self.max_tokens = max_tokens

    async def process(self, prompt: str) -> Dict[str, Any]:
        r = await self.client.chat.completions.create(
            model=self.model_name,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": prompt},
            ],
            temperature=0,
            max_tokens=self.max_tokens,
        )
        return {
            "content": r.choices[0].message.content,
            "tokens":  r.usage.total_tokens if hasattr(r, "usage") else None,
        }


class LlamaClient(_OpenAICompatClient):
    def __init__(self, model_name: str, max_tokens: int = 16384):
        super().__init__(model_name, "llama", max_tokens)


class DeepseekClient(_OpenAICompatClient):
    def __init__(self, model_name: str, max_tokens: int = 16384):
        super().__init__(model_name, "deepseek", max_tokens)

# ----------------------------- FACTORY --------------------------------------

def get_client(llm_type: str, model_name: str, max_tokens: int = 16384) -> Optional[LLMClient]:
    mapping = {
        "openai":    OpenAIClient,
        "anthropic": ClaudeClient,
        "google":    GeminiClient,
        "llama":     LlamaClient,
        "deepseek":  DeepseekClient,
    }
    client_type = MODELS[llm_type]["client_type"]
    cls = mapping.get(client_type)
    if cls is None:
        print(f"Unknown client type: {client_type}")
        return None
    return cls(model_name, max_tokens)

# ----------------------------- ROW PROCESSOR ---------------------------------

async def process_row(
    row, index: int, client: LLMClient,
    existing: set, semaphore: asyncio.Semaphore, timestamp: str
) -> Dict[str, Any]:
    async with semaphore:
        file_id   = row.get("file_id", f"index{index}")
        complaint = row.get("text_content", "")

        if file_id in existing:
            return {"status": "skipped", "file_id": file_id, "reason": "already_saved"}
        if not isinstance(complaint, str) or not complaint.strip():
            return {"status": "skipped", "file_id": file_id, "reason": "empty_text"}

        prompt    = prompt_template.replace("{complaint_text}", complaint)
        start     = time.perf_counter()
        try:
            result    = await client.process(prompt)
            save_path = os.path.join(
                client.output_dir,
                f"{file_id}_{client.model_name.replace('/', '-')}_{timestamp}.txt",
            )
            with open(save_path, "w", encoding="utf-8") as f:
                f.write(result["content"])
            elapsed = time.perf_counter() - start
            return {"status": "success", "file_id": file_id, "model": client.model_name,
                    "time": elapsed, "tokens": result["tokens"]}
        except Exception as e:
            return {"status": "error", "file_id": file_id, "model": client.model_name,
                    "error": str(e), "time": time.perf_counter() - start}

# ----------------------------- MODEL RUNNER ----------------------------------

async def run_model(llm_type: str, model_config: Dict[str, Any], timestamp: str) -> Optional[Dict]:
    if not model_config["enabled"]:
        print(f"  Skipping {llm_type} (disabled)")
        return None

    print(f"\n  Starting {llm_type.upper()} — {model_config['model_name']}")
    t0     = time.perf_counter()
    client = get_client(llm_type, model_config["model_name"], model_config.get("max_tokens", 8192))
    if client is None:
        return None

    existing  = client.get_existing_file_ids()
    semaphore = asyncio.Semaphore(BATCH_SIZE)
    tasks     = [process_row(row, i, client, existing, semaphore, timestamp) for i, row in df.iterrows()]
    raw       = await asyncio.gather(*tasks, return_exceptions=True)

    results = []
    for r in raw:
        if isinstance(r, Exception):
            results.append({"status": "error", "error": str(r)})
        else:
            results.append(r)
            tag = r["file_id"]
            if r["status"] == "success":
                print(f"    [{llm_type}] {tag}  {r['time']:.1f}s  {r.get('tokens','?')} tok")
            elif r["status"] == "error":
                print(f"    [{llm_type}] {tag}  ERROR: {r.get('error')}")

    elapsed       = time.perf_counter() - t0
    successes     = [r for r in results if r["status"] == "success"]
    errors        = [r for r in results if r["status"] == "error"]
    skipped       = [r for r in results if r["status"] == "skipped"]
    avg_time      = sum(r["time"] for r in successes) / len(successes) if successes else 0
    total_tokens  = sum(r.get("tokens") or 0 for r in successes)

    print(f"\n  {llm_type.upper()} done in {elapsed:.1f}s — "
          f"success={len(successes)} error={len(errors)} skipped={len(skipped)} "
          f"tokens={total_tokens:,}")

    summary = {
        "llm_type": llm_type, "model_name": model_config["model_name"],
        "timestamp": timestamp, "total_runtime": elapsed,
        "success_count": len(successes), "error_count": len(errors), "skipped_count": len(skipped),
        "avg_time_per_request": avg_time, "total_tokens": total_tokens, "results": results,
    }
    summary_path = os.path.join(client.output_dir, f"summary_{timestamp}.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return summary

# ----------------------------- MAIN ------------------------------------------

async def main():
    timestamp = datetime.now().strftime("%Y%m%d")

    print(f"\n{'='*70}")
    print("Multi-LLM Extraction Pipeline")
    print(f"{'='*70}")
    print(f"Rows:      {len(df)}")
    print(f"Concurrency: {BATCH_SIZE} per model")
    print(f"Active models: {sum(1 for m in MODELS.values() if m['enabled'])}")
    print(f"Timestamp: {timestamp}")
    print(f"{'='*70}\n")

    t0    = time.perf_counter()
    tasks = [run_model(k, v, timestamp) for k, v in MODELS.items() if v["enabled"]]
    raw   = await asyncio.gather(*tasks, return_exceptions=True)
    total = time.perf_counter() - t0

    summaries = {s["llm_type"]: s for s in raw if s and not isinstance(s, Exception)}

    print(f"\n{'='*70}")
    print(f"Total wall time: {total:.1f}s  |  Models completed: {len(summaries)}")
    print(f"{'='*70}")
    for lt, s in summaries.items():
        print(f"  {lt.upper():12s}  success={s['success_count']}  tokens={s['total_tokens']:,}")

    combined_path = os.path.join("data", f"combined_summary_{timestamp}.json")
    os.makedirs("data", exist_ok=True)
    with open(combined_path, "w", encoding="utf-8") as f:
        json.dump(summaries, f, indent=2)
    print(f"\nCombined summary → {combined_path}\n")


if __name__ == "__main__":
    # Load inputs once at module level so all coroutines share them
    df = pd.read_csv(INPUT_CSV)
    if SAMPLE_SIZE:
        df = df.sample(SAMPLE_SIZE)

    with open(PROMPT_FILE, "r", encoding="utf-8") as fh:
        prompt_template = fh.read()

    os.makedirs(BASE_OUT_DIR, exist_ok=True)
    asyncio.run(main())
