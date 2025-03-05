import openai
import os
from dotenv import load_dotenv
import json

load_dotenv()  # take environment variables from .env.

client = openai.OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),  # This is the default and can be omitted
)

def explain_error(error, patch):
    try:
        response = client.chat.completions.create(
            messages=[
                {"role": "user", "content": f'''
        You are an experienced PostgreSQL developer and security expert. We have observed an error 
                 in a Postgres build on one of the buildfarm server animals and we would
                 like your assessment if the patch we have in mind is responsible. 

            You are to read the error and patch and then give an assessment of the following:
                 1. On a scale of 0-10, how likely is it that the patch directly caused the error?
                 2. If applicable, explain how the patch caused the error, else N/A
                 3. If applicable, suggest how to fix the patch, else N/A
            
                 Respond in a JSON object with NO other surrounding text or symbols in the following format:
                 {{
                    "score": <0-10>,
                    "explanation": "text",
                    "fix": "text"
                 }}

                 ===

                Now, here is the error log:
                 
                 {
                     error
                 }

                ===

                Now, here is the code patch:

                {
                    patch
                }

            ======

                Okay, you've seen the logs, read the code patch. Now please respond as we requested above:

                a JSON object with NO other surrounding text or symbols in the following format:
                 {{
                    "score": <0-10>,
                    "explanation": "text",
                    "fix": "text"
                 }}


    '''}
            
            ],
            reasoning_effort="high",
            model="o3-mini-2025-01-31",
        )

        text = response.choices[0].message.content

        return json.loads(text)
    except:
        return None 
