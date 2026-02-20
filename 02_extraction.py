# -----------------------------------------------------------------------------
## Author: Elijah Appelson
## Date: February 20th, 2026
## Summary: This script automates the extraction of information from a collection 
## of complaints using multiple Large Language Models (LLMs) in parallel. Each LLM 
## processes the documents asynchronously, generating JSON-formatted outputs that 
## are saved as text files. The pipeline supports OpenAI, Anthropic, Google Gemini, 
## LLaMa, and DeepSeek models. Detailed per-model and combined execution summaries,
## including runtime, token usage, and success/error counts, are automatically generated.
# -----------------------------------------------------------------------------

# Importing Libraries
import os
import json
import time
import asyncio
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional
import config

# !python -m pip install config
# !python -m pip install openai
# !python -m pip install anthropic
# !python -m pip install google.generativeai

# Importing LLMs
from openai import AsyncOpenAI
from anthropic import AsyncAnthropic
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold

# File Paths
PROMPT_FILE = "prompt.txt"
INPUT_CSV = "data/filtered_texts.csv"
BASE_OUTPUT_DIR = "data/"

# Processing Parameters
BATCH_SIZE = 15
BATCH_DELAY = 0.1
sample_size = 500

# ---------------------------- CONFIGURATION ----------------------------------

# Model Configs, all with 8192 tokens at maximum
MODELS = {
  
    # OpenAi
    "openai": {
        "model_name": "gpt-4o-mini",
        "enabled": True,
        "client_type": "openai",
        "max_tokens": 16384
    },
    
    # Claude
    "claude": {
        "model_name": "claude-3-5-sonnet-20241022",
        "enabled": False,
        "client_type": "anthropic",
        "max_tokens": 16384
    },
    
    # Gemini
    "gemini": {
        "model_name": "gemini-2.5-flash-lite",
        "enabled": False,
        "client_type": "google",
        "max_tokens": 16384
    },
    
    # LLaMa
    "llama": {
        "model_name": "meta-llama/Llama-3.3-70B-Instruct",
        "enabled": False,
        "client_type": "llama",
        "max_tokens": 16384
    },
    
    # Deepseek
    "deepseek": {
        "model_name": "deepseek-ai/DeepSeek-V3.2:novita",
        "enabled": False,
        "client_type": "deepseek",
        "max_tokens": 16384
    }
}

# --------------------------------- SETUP --------------------------------------

# Getting the time
timestamp = datetime.now().strftime("%Y%m%d")

# Loading the data
df = pd.read_csv(INPUT_CSV).sample(sample_size)

# Loading the prompt template
with open(PROMPT_FILE, "r", encoding="utf-8") as f:
    prompt_template = f.read()

# ------------------------- CLIENT TEMPLATES -----------------------------------

class LLMClient:
    
    # Class for LLMs
    def __init__(self, model_name: str, llm_type: str):
        self.model_name = model_name
        self.llm_type = llm_type
        self.output_dir = os.path.join(BASE_OUTPUT_DIR, f"{llm_type}_extracted_text")
        os.makedirs(self.output_dir, exist_ok=True)
        
    # Removing already processed files
    def get_existing_files(self) -> set:
        existing_file_ids = set()
        for fname in os.listdir(self.output_dir):
            if not fname.endswith(".txt"):
                continue
            parts = fname.split("_")
            if len(parts) >= 1:
                existing_file_ids.add(parts[0])
        return existing_file_ids
    
    async def process(self, prompt: str) -> str:
        raise NotImplementedError

# Defining OpenAI client
class OpenAIClient(LLMClient):

    def __init__(self, model_name: str, max_tokens: int = 8192):
        super().__init__(model_name, "openai")
        self.client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        self.max_tokens = max_tokens
    
    async def process(self, prompt: str) -> Dict[str, Any]:
        response = await self.client.chat.completions.create(
            model=self.model_name,
            messages=[
                {"role": "system", "content": "You are a legal data extraction system. Respond ONLY with valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=self.max_tokens
        )
        
        return {
            "content": response.choices[0].message.content,
            "tokens": response.usage.total_tokens if hasattr(response, 'usage') else None
        }

# Defining Anthropic Client
class ClaudeClient(LLMClient):

    def __init__(self, model_name: str, max_tokens: int = 8192):
        super().__init__(model_name, "claude")
        self.client = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
        self.max_tokens = max_tokens
    
    async def process(self, prompt: str) -> Dict[str, Any]:
        response = await self.client.messages.create(
            model=self.model_name,
            max_tokens=self.max_tokens,
            temperature=0,
            system="You are a legal data extraction system. Respond ONLY with valid JSON.",
            messages=[{"role": "user", "content": prompt}]
        )
        
        return {
            "content": response.content[0].text,
            "tokens": response.usage.input_tokens + response.usage.output_tokens
        }

# Defining Gemini Client
class GeminiClient(LLMClient):

    def __init__(self, model_name: str, max_tokens: int = 8192):
        super().__init__(model_name, "gemini")
        genai.configure(api_key=GOOGLE_API_KEY)
        
        # Removing safety blocks
        self.safety_settings = {
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
        }
        
        self.model = genai.GenerativeModel(
            model_name=model_name,
            safety_settings=self.safety_settings,
            
            # Defining temperature and top p and k
            generation_config={
                "temperature": 0,
                "top_p": 1,
                "top_k": 1,
                "max_output_tokens": max_tokens,
            }
        )
    
    async def process(self, prompt: str) -> Dict[str, Any]:
        loop = asyncio.get_event_loop()
        
        # Manually adding in a prompt
        full_prompt = (
            "You are a legal data extraction system. Respond ONLY with valid JSON.\n\n"
            f"{prompt}"
        )
        
        response = await loop.run_in_executor(
            None,
            lambda: self.model.generate_content(full_prompt)
        )
        
        tokens = None
        if hasattr(response, 'usage_metadata'):
            tokens = (response.usage_metadata.prompt_token_count + 
                     response.usage_metadata.candidates_token_count)
        
        return {
            "content": response.text,
            "tokens": tokens
        }

# Defining LLaMa client
class LlamaClient(LLMClient):

    def __init__(self, model_name: str, max_tokens: int = 8192):
        super().__init__(model_name, "llama")
        self.client = AsyncOpenAI(
            api_key=HUGGINGFACE_API_KEY,
            base_url="https://router.huggingface.co/v1"
        )
        self.max_tokens = max_tokens
    
    async def process(self, prompt: str) -> Dict[str, Any]:
        response = await self.client.chat.completions.create(
            model=self.model_name,
            messages=[
                {"role": "system", "content": "You are a legal data extraction system. Respond ONLY with valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=self.max_tokens
        )
        
        return {
            "content": response.choices[0].message.content,
            "tokens": response.usage.total_tokens if hasattr(response, 'usage') else None
        }

# Defining DeepSeek client
class DeepseekClient(LLMClient):

    def __init__(self, model_name: str, max_tokens: int = 8192):
        super().__init__(model_name, "deepseek")
        self.client = AsyncOpenAI(
            api_key=HUGGINGFACE_API_KEY,
            base_url="https://router.huggingface.co/v1"
        )
        self.max_tokens = max_tokens
    
    async def process(self, prompt: str) -> Dict[str, Any]:
        response = await self.client.chat.completions.create(
            model=self.model_name,
            messages=[
                {"role": "system", "content": "You are a legal data extraction system. Respond ONLY with valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=self.max_tokens
        )
        
        return {
            "content": response.choices[0].message.content,
            "tokens": response.usage.total_tokens if hasattr(response, 'usage') else None
        }


# ---------------------------- PROCESSING LOGIC --------------------------------

# Creating LLM client
def get_client(llm_type: str, model_name: str, max_tokens: int = 16384) -> Optional[LLMClient]:

    client_type = MODELS[llm_type]["client_type"]
    
    if client_type == "openai":
        return OpenAIClient(model_name, max_tokens)
    elif client_type == "anthropic":
        return ClaudeClient(model_name, max_tokens)
    elif client_type == "google":
        return GeminiClient(model_name, max_tokens)
    elif client_type == "llama":
        return LlamaClient(model_name, max_tokens)
    elif client_type == "deepseek":
        return DeepseekClient(model_name, max_tokens)
    else:
        print(f"Unknown client type: {client_type}")
        return None

# Processing a single row at a time
async def process_single_row(
    row, 
    index: int, 
    client: LLMClient, 
    existing_files: set,
    semaphore: asyncio.Semaphore
) -> Dict[str, Any]:

    async with semaphore:
        file_id = row.get("file_id", f"index{index}")
        
        # Skipping if exists already
        if file_id in existing_files:
            return {
                "status": "skipped",
                "file_id": file_id,
                "llm_type": client.llm_type,
                "reason": "already_saved"
            }
        
        # Getting complaint text from the row
        complaint = row["text_content"]
        
        # If doesn't exist or is empty then skipping
        if not isinstance(complaint, str) or len(complaint) == 0:
            return {
                "status": "skipped",
                "file_id": file_id,
                "llm_type": client.llm_type,
                "reason": "empty_text"
            }
        
        # Adding the complaint text and prompt
        extraction_prompt = prompt_template.replace("{complaint_text}", complaint)
        
        # Starting timer
        start_time = time.perf_counter()
        
        # Getting the client response
        try:
            result = await client.process(extraction_prompt)
            output_text = result["content"]
            
            
            # Saving the output with the model name and time
            save_path = os.path.join(
                client.output_dir,
                f"{file_id}_{client.model_name.replace('/', '-')}_{timestamp}.txt"
            )
            
            # Saving as a text file
            with open(save_path, "w", encoding="utf-8") as f:
                f.write(output_text)
            
            # Time taken
            elapsed = time.perf_counter() - start_time
            
            # Returning result
            return {
                "status": "success",
                "file_id": file_id,
                "llm_type": client.llm_type,
                "model": client.model_name,
                "time": elapsed,
                "tokens": result["tokens"]
            }
            
        except Exception as e:
            elapsed = time.perf_counter() - start_time
            return {
                "status": "error",
                "file_id": file_id,
                "llm_type": client.llm_type,
                "model": client.model_name,
                "error": str(e),
                "time": elapsed
            }

# Processing rows with a single LLM
async def process_with_llm(llm_type: str, model_config: Dict[str, Any]) -> Dict[str, Any]:
    if not model_config["enabled"]:
        print(f"Skipping {llm_type.upper()} (disabled)")
        return None
    
    print(f"\nStarting {llm_type.upper()} extraction")
    print(f"Model: {model_config['model_name']}")
    
    total_start = time.perf_counter()
    
    try:
        max_tokens = model_config.get('max_tokens', 8192)
        client = get_client(llm_type, model_config["model_name"], max_tokens)
        if client is None:
            return None
        
        existing_files = client.get_existing_files()
        semaphore = asyncio.Semaphore(BATCH_SIZE)
        
        tasks = [
            process_single_row(row, i, client, existing_files, semaphore)
            for i, row in df.iterrows()
        ]
        
        # REPLACE WITH THIS:
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        results = []
        for result in raw_results:
            if isinstance(result, Exception):
                results.append({"status": "error", "error": str(result), "llm_type": llm_type})
            else:
                results.append(result)
                if result["status"] == "success":
                    print(f"  [{llm_type}] {result['file_id']} completed in {result['time']:.2f}s ({result.get('tokens', 'N/A')} tokens)")
                elif result["status"] == "error":
                    print(f"  [{llm_type}] {result['file_id']} error: {result.get('error', 'Unknown')}")
        
        total_end = time.perf_counter()
        
        success_count = sum(1 for r in results if r.get("status") == "success")
        error_count = sum(1 for r in results if r.get("status") == "error")
        skipped_count = sum(1 for r in results if r.get("status") == "skipped")
        success_times = [r["time"] for r in results if r.get("status") == "success"]
        avg_time = sum(success_times) / len(success_times) if success_times else 0
        total_tokens = sum(r.get("tokens", 0) or 0 for r in results if r.get("status") == "success")
        
        print(f"\n{llm_type.upper()} Results:")
        print(f"  Runtime: {total_end - total_start:.2f}s")
        print(f"  Success: {success_count} | Errors: {error_count} | Skipped: {skipped_count}")
        print(f"  Avg time per file: {avg_time:.2f}s")
        print(f"  Total tokens: {total_tokens:,}")
        if total_end - total_start > 0:
            print(f"  Throughput: {success_count / (total_end - total_start):.2f} files/sec")
        
        summary = {
            "llm_type": llm_type,
            "model_name": model_config["model_name"],
            "timestamp": timestamp,
            "total_runtime": total_end - total_start,
            "success_count": success_count,
            "error_count": error_count,
            "skipped_count": skipped_count,
            "avg_time_per_request": avg_time,
            "total_tokens": total_tokens,
            "results": results
        }
        
        summary_path = os.path.join(client.output_dir, f"summary_{timestamp}.json")
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        
        return summary
        
    except Exception as e:
        print(f"ERROR: {llm_type.upper()} processing failed - {str(e)}")
        return None

# Main execution function
async def main():

    print(f"\n{'='*70}")
    print(f"Multi-LLM Extraction Pipeline")
    print(f"{'='*70}")
    print(f"Total files: {len(df)}")
    print(f"Concurrency: {BATCH_SIZE} requests per model")
    print(f"Active models: {sum(1 for c in MODELS.values() if c['enabled'])}")
    print(f"Timestamp: {timestamp}")
    print(f"{'='*70}\n")
    
    overall_start = time.perf_counter()
    
    # Create tasks for all enabled LLMs to run in parallel
    tasks = [
        process_with_llm(llm_type, model_config)
        for llm_type, model_config in MODELS.items()
        if model_config["enabled"]
    ]
    
    # Run all LLMs simultaneously
    all_summaries_list = await asyncio.gather(*tasks, return_exceptions=True)
    
    overall_end = time.perf_counter()
    
    # Process results
    all_summaries = {}
    for summary in all_summaries_list:
        if isinstance(summary, Exception):
            print(f"Error in LLM processing: {str(summary)}")
        elif summary is not None:
            all_summaries[summary["llm_type"]] = summary
    
    print(f"\n{'='*70}")
    print(f"Overall Summary")
    print(f"{'='*70}")
    print(f"Total execution time: {overall_end - overall_start:.2f}s")
    print(f"Models completed: {len(all_summaries)}")
    print(f"{'='*70}\n")
    
    for llm_type, summary in all_summaries.items():
        print(f"{llm_type.upper()}:")
        print(f"  Model: {summary['model_name']}")
        print(f"  Successful extractions: {summary['success_count']}")
        print(f"  Runtime: {summary['total_runtime']:.2f}s")
        print(f"  Tokens used: {summary['total_tokens']:,}")
        print()
    
    combined_summary_path = os.path.join(BASE_OUTPUT_DIR, f"combined_summary_{timestamp}.json")
    with open(combined_summary_path, "w", encoding="utf-8") as f:
        json.dump(all_summaries, f, indent=2)
    
    print(f"Summary saved: {combined_summary_path}\n")

# ------------------------------- RUNNING --------------------------------------

if __name__ == "__main__":
    asyncio.run(main())
    
