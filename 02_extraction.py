# =============================================================================
# 02_extract.py
# =============================================================================

import os
import json
import time
import asyncio
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional

from openai import AsyncOpenAI
from anthropic import AsyncAnthropic
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold

# ----------------------------- CONFIG ----------------------------------------

with open("config.json") as f:
    cfg = json.load(f)

DATA_DIR      = cfg["paths"]["data_dir"]
EXTRACT_DIR   = cfg["paths"]["extract_dir"]
PROMPT_FILE   = cfg["paths"]["prompt_file"]
FILTERED_CSV  = os.path.join(DATA_DIR, "filtered_texts.csv")

SAMPLE_SIZE   = cfg["parameters"]["sample_size"]   # null in config.json = all rows
BATCH_SIZE    = cfg["parameters"]["batch_size"]
BATCH_DELAY   = cfg["parameters"]["batch_delay"]
MAX_TOKENS    = cfg["parameters"]["max_tokens"]

MODELS = {k: {**v, "max_tokens": MAX_TOKENS} for k, v in cfg["models"].items()}

OPENAI_API_KEY      = os.getenv("OPENAI_API_KEY",      "")
ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY",   "")
GOOGLE_API_KEY      = os.getenv("GOOGLE_API_KEY",      "")
HUGGINGFACE_API_KEY = os.getenv("HUGGINGFACE_API_KEY", "")

SYSTEM_PROMPT = "You are a legal data extraction system. Respond ONLY with valid JSON."

# ----------------------------- CLIENT BASE -----------------------------------

class LLMClient:
    def __init__(self, model_name: str, llm_type: str):
        self.model_name = model_name
        self.llm_type   = llm_type
        self.output_dir = os.path.join(EXTRACT_DIR, llm_type)
        os.makedirs(self.output_dir, exist_ok=True)

    def get_existing_file_ids(self) -> set:
        return {f.split("_")[0] for f in os.listdir(self.output_dir) if f.endswith(".txt")}

    async def process(self, prompt: str) -> Dict[str, Any]:
        raise NotImplementedError

# ----------------------------- CLIENT IMPLEMENTATIONS ------------------------

class OpenAIClient(LLMClient):
    def __init__(self, model_name, max_tokens):
        super().__init__(model_name, "openai")
        self.client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        self.max_tokens = max_tokens

    async def process(self, prompt):
        r = await self.client.chat.completions.create(
            model=self.model_name,
            messages=[{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": prompt}],
            temperature=0, max_tokens=self.max_tokens)
        return {"content": r.choices[0].message.content, "tokens": r.usage.total_tokens}


class ClaudeClient(LLMClient):
    def __init__(self, model_name, max_tokens):
        super().__init__(model_name, "claude")
        self.client = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
        self.max_tokens = max_tokens

    async def process(self, prompt):
        r = await self.client.messages.create(
            model=self.model_name, max_tokens=self.max_tokens, temperature=0,
            system=SYSTEM_PROMPT, messages=[{"role": "user", "content": prompt}])
        return {"content": r.content[0].text, "tokens": r.usage.input_tokens + r.usage.output_tokens}


class GeminiClient(LLMClient):
    def __init__(self, model_name, max_tokens):
        super().__init__(model_name, "gemini")
        genai.configure(api_key=GOOGLE_API_KEY)
        safety = {c: HarmBlockThreshold.BLOCK_NONE for c in [
            HarmCategory.HARM_CATEGORY_HARASSMENT, HarmCategory.HARM_CATEGORY_HATE_SPEECH,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT, HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT]}
        self.model = genai.GenerativeModel(model_name=model_name, safety_settings=safety,
            generation_config={"temperature": 0, "top_p": 1, "top_k": 1, "max_output_tokens": max_tokens})

    async def process(self, prompt):
        loop = asyncio.get_event_loop()
        r = await loop.run_in_executor(None, lambda: self.model.generate_content(f"{SYSTEM_PROMPT}\n\n{prompt}"))
        tokens = (r.usage_metadata.prompt_token_count + r.usage_metadata.candidates_token_count
                  if hasattr(r, "usage_metadata") else None)
        return {"content": r.text, "tokens": tokens}


class _HFClient(LLMClient):
    def __init__(self, model_name, llm_type, max_tokens):
        super().__init__(model_name, llm_type)
        self.client = AsyncOpenAI(api_key=HUGGINGFACE_API_KEY, base_url="https://router.huggingface.co/v1")
        self.max_tokens = max_tokens

    async def process(self, prompt):
        r = await self.client.chat.completions.create(
            model=self.model_name,
            messages=[{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": prompt}],
            temperature=0, max_tokens=self.max_tokens)
        return {"content": r.choices[0].message.content, "tokens": r.usage.total_tokens}

class LlamaClient(_HFClient):
    def __init__(self, model_name, max_tokens): super().__init__(model_name, "llama", max_tokens)

class DeepseekClient(_HFClient):
    def __init__(self, model_name, max_tokens): super().__init__(model_name, "deepseek", max_tokens)

# ----------------------------- FACTORY --------------------------------------

def get_client(llm_type, model_name, max_tokens):
    return {
        "openai":    OpenAIClient,
        "anthropic": ClaudeClient,
        "google":    GeminiClient,
        "llama":     LlamaClient,
        "deepseek":  DeepseekClient,
    }[MODELS[llm_type]["client_type"]](model_name, max_tokens)

# ----------------------------- ROW PROCESSOR ---------------------------------

async def process_row(row, index, client, existing, semaphore, timestamp):
    async with semaphore:
        file_id   = row.get("file_id", f"index{index}")
        complaint = row.get("text_content", "")
        if file_id in existing:
            return {"status": "skipped", "file_id": file_id, "reason": "already_saved"}
        if not isinstance(complaint, str) or not complaint.strip():
            return {"status": "skipped", "file_id": file_id, "reason": "empty_text"}
        prompt = prompt_template.replace("{complaint_text}", complaint)
        start  = time.perf_counter()
        try:
            result = await client.process(prompt)
            path   = os.path.join(client.output_dir,
                                  f"{file_id}_{client.model_name.replace('/', '-')}_{timestamp}.txt")
            with open(path, "w", encoding="utf-8") as f:
                f.write(result["content"])
            return {"status": "success", "file_id": file_id, "model": client.model_name,
                    "time": time.perf_counter() - start, "tokens": result["tokens"]}
        except Exception as e:
            return {"status": "error", "file_id": file_id, "error": str(e),
                    "time": time.perf_counter() - start}

# ----------------------------- MODEL RUNNER ----------------------------------

async def run_model(llm_type, model_config, timestamp):
    if not model_config["enabled"]:
        return None
    print(f"\n  {llm_type.upper()} — {model_config['model_name']}")
    t0        = time.perf_counter()
    client    = get_client(llm_type, model_config["model_name"], model_config["max_tokens"])
    existing  = client.get_existing_file_ids()
    semaphore = asyncio.Semaphore(BATCH_SIZE)
    tasks     = [process_row(row, i, client, existing, semaphore, timestamp) for i, row in df.iterrows()]
    raw       = await asyncio.gather(*tasks, return_exceptions=True)
    results   = [{"status": "error", "error": str(r)} if isinstance(r, Exception) else r for r in raw]

    successes = [r for r in results if r["status"] == "success"]
    errors    = [r for r in results if r["status"] == "error"]
    skipped   = [r for r in results if r["status"] == "skipped"]
    elapsed   = time.perf_counter() - t0
    tokens    = sum(r.get("tokens") or 0 for r in successes)

    print(f"  done {elapsed:.1f}s — success={len(successes)} error={len(errors)} skipped={len(skipped)} tokens={tokens:,}")

    summary = {"llm_type": llm_type, "model_name": model_config["model_name"], "timestamp": timestamp,
               "total_runtime": elapsed, "success_count": len(successes), "error_count": len(errors),
               "skipped_count": len(skipped), "total_tokens": tokens, "results": results}
    with open(os.path.join(client.output_dir, f"summary_{timestamp}.json"), "w") as f:
        json.dump(summary, f, indent=2)
    return summary

# ----------------------------- MAIN ------------------------------------------

async def main():
    timestamp = datetime.now().strftime("%Y%m%d")
    print(f"\nExtracting {len(df)} rows | {sum(1 for m in MODELS.values() if m['enabled'])} models active")
    t0   = time.perf_counter()
    raw  = await asyncio.gather(*[run_model(k, v, timestamp) for k, v in MODELS.items() if v["enabled"]],
                                 return_exceptions=True)
    summaries = {s["llm_type"]: s for s in raw if s and not isinstance(s, Exception)}
    print(f"\nTotal: {time.perf_counter()-t0:.1f}s")
    for lt, s in summaries.items():
        print(f"  {lt}: success={s['success_count']} tokens={s['total_tokens']:,}")
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(os.path.join(DATA_DIR, f"combined_summary_{timestamp}.json"), "w") as f:
        json.dump(summaries, f, indent=2)

if __name__ == "__main__":
    df = pd.read_csv(FILTERED_CSV)
    if SAMPLE_SIZE:
        df = df.sample(SAMPLE_SIZE)
    with open(PROMPT_FILE) as f:
        prompt_template = f.read()
    os.makedirs(EXTRACT_DIR, exist_ok=True)
    asyncio.run(main())
