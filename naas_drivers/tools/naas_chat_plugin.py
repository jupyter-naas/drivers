import tiktoken
import json

MODELS = {
    "gpt-3.5-turbo": 4097,
    "gpt-3.5-turbo-16k": 16385,
    "gpt-4": 8192,
}


class NaasChatPlugin:
    def num_tokens_from_string(self, string: str, encoding_name="cl100k_base") -> int:
        """
        Returns the number of tokens in a text string.

        This function uses the specified encoding to tokenize the input string, and then returns the number of tokens.

        Parameters:
        - string (str): The input string to be tokenized. By default, "cl100k_base"
        - encoding_name (str): The name of the encoding to be used for tokenization.

        Returns:
        int: The number of tokens in the input string.
        """
        encoding = tiktoken.get_encoding(encoding_name)
        num_tokens = len(encoding.encode(string))
        return num_tokens

    def check_tokens(self, prompt, model, limit=0.2):
        """
        Checks the number of tokens in the prompt and warns if it exceeds the maximum limit or the recommended limit.

        This function calculates the number of tokens in the prompt using the specified model's encoding.
        It then compares this number with the model's maximum limit and a recommended limit (default is 20% of the maximum).
        If the number of tokens exceeds either limit, a warning is printed.

        Parameters:
        - prompt (str): The input prompt to be checked.
        - model (str): The name of the model to be used for tokenization.
        Must be one of 'gpt-3.5-turbo', 'gpt-3.5-turbo-16k', 'gpt-4'
        - limit (float): The recommended limit as a fraction of the maximum limit. Default is 0.2 (20%).

        Returns:
        int: The number of tokens in the prompt.
        int: The number of max tokens allowed by the model.
        """
        # Check if the model is in the MODELS dictionary
        if model not in list(MODELS.keys()):
            print(f"⛔ Model {model} not found. Must be one of {list(MODELS.keys())}")
            return None, None
        # Get max tokens
        max_tokens = MODELS.get(model)

        # Limit recommended
        recommended_limit = int(max_tokens * limit)

        # Check tokens
        prompt_tokens = self.num_tokens_from_string(prompt, "cl100k_base")
        if prompt_tokens >= max_tokens:
            print(
                f"""⛔ Be careful, your system prompt is too big.
                Exceeded max tokens allowed by models (max_tokens={max_tokens}, system_tokens={prompt_tokens})"""
            )
        elif prompt_tokens > recommended_limit:
            print(
                f"""⚠️ Be careful, your system prompt looks too big.
                Tokens: {prompt_tokens} (limit recommended: {int(limit*100)}% -> {recommended_limit})"""
            )
        else:
            print(
                f"""
                ✅ System prompt tokens count OK: {prompt_tokens} (limit: {int(limit*100)}% -> {recommended_limit})
                """
            )
        return prompt_tokens, max_tokens

    def create_plugin(
        self, name, prompt, model="gpt-3.5-turbo-16k", temperature=0, output_path=None
    ):
        """
        Creates a JSON file for a chat plugin with specified parameters and saves it to the specified output path.

        This function checks the number of tokens in the prompt, creates a JSON object, and saves it to a JSON file.
        It then creates an asset with the JSON file and returns the asset link.

        Parameters:
        - name (str): The name of the plugin.
        - prompt (str): The prompt for the plugin.
        - model (str): The name of the model to be used for tokenization. Default is "gpt-3.5-turbo-16k".
        - temperature (int): The temperature parameter for the model. Default is 0.
        - output_path (str): The path where the JSON file should be saved. If not provided, it will be created from the plugin name.

        Returns:
        str: The output path of the naas chat plugin.
        """
        # Create output path
        if not output_path:
            output_path = name.lower().replace(" ", "_") + "_plugin.json"
        # Check tokens
        prompt_tokens, max_tokens = self.check_tokens(prompt, model)

        # Create json
        plugin = {
            "name": name,
            "model": model,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "prompt": prompt,
        }

        # Save dict to JSON file
        with open(output_path, "w") as f:
            json.dump(plugin, f)
        print(
            f"💾 Plugin successfully saved. You can use it in your Naas Chat with: {output_path}"
        )
        return output_path
