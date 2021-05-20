from typing import Optional, Union

from transformers import PretrainedConfig, PreTrainedTokenizer

from naas_drivers.driver import InDriver
from transformers.pipelines.question_answering import QuestionAnsweringPipeline
from transformers.pipelines.text2text_generation import SummarizationPipeline, Text2TextGenerationPipeline
from transformers.pipelines.text_classification import TextClassificationPipeline
from transformers.pipelines.text_generation import TextGenerationPipeline
from transformers.file_utils import is_torch_available, is_tf_available

if is_torch_available():
    import torch
    from transformers.models.auto.modeling_auto import (
        AutoModelForSequenceClassification,
        AutoModelForSeq2SeqLM,
        AutoModelForCausalLM,
        AutoModelForQuestionAnswering,
    )

if is_tf_available():
    import tensorflow as tf
    from transformers.models.auto.modeling_tf_auto import (
        TFAutoModelForSequenceClassification,
        TFAutoModelForSeq2SeqLM,
        TFAutoModelForCausalLM,
        TFAutoModelForQuestionAnswering,
    )

TASKS = {
    "text-classification": {
        "impl": TextClassificationPipeline,
        "tf": TFAutoModelForSequenceClassification if is_tf_available() else None,
        "pt": AutoModelForSequenceClassification if is_torch_available() else None,
        "default": {
            "model": {
                "pt": "distilbert-base-uncased-finetuned-sst-2-english",
                "tf": "distilbert-base-uncased-finetuned-sst-2-english",
            },
        },
    },
    "text2text-generation": {
        "impl": Text2TextGenerationPipeline,
        "tf": TFAutoModelForSeq2SeqLM if is_tf_available() else None,
        "pt": AutoModelForSeq2SeqLM if is_torch_available() else None,
        "default": {"model": {"pt": "t5-base", "tf": "t5-base"}},
    },
    "text-generation": {
        "impl": TextGenerationPipeline,
        "tf": TFAutoModelForCausalLM if is_tf_available() else None,
        "pt": AutoModelForCausalLM if is_torch_available() else None,
        "default": {"model": {"pt": "gpt2", "tf": "gpt2"}},
    },
    "question-answering": {
        "impl": QuestionAnsweringPipeline,
        "tf": TFAutoModelForQuestionAnswering if is_tf_available() else None,
        "pt": AutoModelForQuestionAnswering if is_torch_available() else None,
        "default": {
            "model": {"pt": "distilbert-base-cased-distilled-squad", "tf": "distilbert-base-cased-distilled-squad"},
        },
    },
    "summarization": {
        "impl": SummarizationPipeline,
        "tf": TFAutoModelForSeq2SeqLM if is_tf_available() else None,
        "pt": AutoModelForSeq2SeqLM if is_torch_available() else None,
        "default": {"model": {"pt": "sshleifer/distilbart-cnn-12-6", "tf": "t5-small"}},
    },

}


class NLP(InDriver):
    def get(
            self,
            task: str,
            model: Optional = None,
            config: Optional[Union[str, PretrainedConfig]] = None,
            tokenizer: Optional[Union[str, PreTrainedTokenizer]] = None,
    ):
        if task not in TASKS:
            raise KeyError(
                "Unknown task {}, available tasks are {}".format(
                    task, list(TASKS.keys())
                )
            )
