from typing import Optional, Union, Dict, Any

from transformers import PretrainedConfig, PreTrainedTokenizer, AutoConfig

from naas_drivers.driver import InDriver
from transformers.pipelines.question_answering import QuestionAnsweringPipeline
from transformers.pipelines.text2text_generation import SummarizationPipeline, Text2TextGenerationPipeline
from transformers.pipelines.text_classification import TextClassificationPipeline
from transformers.pipelines.text_generation import TextGenerationPipeline
from transformers.file_utils import is_torch_available, is_tf_available
from transformers.pipelines.base import infer_framework_from_model
from transformers.utils import logging

logger = logging.get_logger(__name__)

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
            framework: Optional[str] = None,
            revision: Optional[str] = None,
            model_kwargs: Dict[str, Any] = {},
            **kwargs
    ):
        if task not in TASKS:
            raise KeyError(
                "Unknown task {}, available tasks are {}".format(
                    task, list(TASKS.keys())
                )
            )

        targeted_task = TASKS[task]
        task_class = targeted_task["impl"]

        if model is None:
            model = targeted_task["default"]["model"]

        if framework is None:
            framework, model = infer_framework_from_model(model, targeted_task, revision=revision, task=task)

        task_class, model_class = targeted_task["impl"], targeted_task[framework]
        if isinstance(config, str):
            config = AutoConfig.from_pretrained(config, revision=revision, _from_pipeline=task, **model_kwargs)

        if isinstance(model, str):
            # Handle transparent TF/PT model conversion
            if framework == "pt" and model.endswith(".h5"):
                model_kwargs["from_tf"] = True
                logger.warning(
                    "Model might be a TensorFlow model (ending with `.h5`) but TensorFlow is not available. "
                    "Trying to load the model with PyTorch."
                )
            elif framework == "tf" and model.endswith(".bin"):
                model_kwargs["from_pt"] = True
                logger.warning(
                    "Model might be a PyTorch model (ending with `.bin`) but PyTorch is not available. "
                    "Trying to load the model with Tensorflow."
                )

            if model_class is None:
                raise ValueError(
                    f"Pipeline using {framework} framework, but this framework is not supported by this pipeline."
                )

            model = model_class.from_pretrained(
                model, config=config, revision=revision, _from_pipeline=task, **model_kwargs
            )

        return task_class(model=model, framework=framework, task=task, **kwargs)
