# flake8: noqa

from naas_drivers.driver import InDriver
from typing import Optional, Union, Dict, Any

from transformers import (
    PretrainedConfig,
    PreTrainedTokenizer,
    AutoConfig,
    AutoTokenizer,
)

from transformers.pipelines.question_answering import QuestionAnsweringPipeline
from transformers.pipelines.text2text_generation import (
    SummarizationPipeline,
    Text2TextGenerationPipeline,
    TranslationPipeline,
)
from transformers.pipelines.text_classification import TextClassificationPipeline
from transformers.pipelines.text_generation import TextGenerationPipeline
from transformers.pipelines.feature_extraction import FeatureExtractionPipeline
from transformers.pipelines.token_classification import TokenClassificationPipeline
from transformers.pipelines.fill_mask import FillMaskPipeline
from transformers.pipelines.table_question_answering import (
    TableQuestionAnsweringPipeline,
)
from transformers.pipelines.zero_shot_classification import (
    ZeroShotClassificationPipeline,
)
from transformers.pipelines.conversational import ConversationalPipeline
from transformers.pipelines.image_classification import ImageClassificationPipeline
from transformers.pipelines.base import infer_framework_from_model, Pipeline
from transformers.utils import logging
from transformers.file_utils import is_torch_available, is_tf_available


logger = logging.get_logger(__name__)

if is_torch_available():
    import torch
    from transformers.models.auto.modeling_auto import (
        AutoModelForSequenceClassification,
        AutoModelForSeq2SeqLM,
        AutoModelForCausalLM,
        AutoModelForQuestionAnswering,
        AutoModelForMaskedLM,
        AutoModel,
        AutoModelForTokenClassification,
        AutoModelForTableQuestionAnswering,
        AutoModelForImageClassification,
    )

if is_tf_available():
    import tensorflow as tf
    from transformers.models.auto.modeling_tf_auto import (
        TFAutoModelForSequenceClassification,
        TFAutoModelForSeq2SeqLM,
        TFAutoModelForCausalLM,
        TFAutoModelForQuestionAnswering,
        TFAutoModelForMaskedLM,
        TFAutoModel,
        TFAutoModelForTokenClassification,
    )


TASKS = {
    "feature-extraction": {
        "impl": FeatureExtractionPipeline,
        "tf": TFAutoModel if is_tf_available() else None,
        "pt": AutoModel if is_torch_available() else None,
        "default": {
            "model": {"pt": "distilbert-base-cased", "tf": "distilbert-base-cased"}
        },
    },
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
    "token-classification": {
        "impl": TokenClassificationPipeline,
        "tf": TFAutoModelForTokenClassification if is_tf_available() else None,
        "pt": AutoModelForTokenClassification if is_torch_available() else None,
        "default": {
            "model": {
                "pt": "dbmdz/bert-large-cased-finetuned-conll03-english",
                "tf": "dbmdz/bert-large-cased-finetuned-conll03-english",
            },
        },
    },
    "question-answering": {
        "impl": QuestionAnsweringPipeline,
        "tf": TFAutoModelForQuestionAnswering if is_tf_available() else None,
        "pt": AutoModelForQuestionAnswering if is_torch_available() else None,
        "default": {
            "model": {
                "pt": "distilbert-base-cased-distilled-squad",
                "tf": "distilbert-base-cased-distilled-squad",
            },
        },
    },
    "table-question-answering": {
        "impl": TableQuestionAnsweringPipeline,
        "pt": AutoModelForTableQuestionAnswering if is_torch_available() else None,
        "tf": None,
        "default": {
            "model": {
                "pt": "google/tapas-base-finetuned-wtq",
                "tokenizer": "google/tapas-base-finetuned-wtq",
                "tf": "google/tapas-base-finetuned-wtq",
            },
        },
    },
    "fill-mask": {
        "impl": FillMaskPipeline,
        "tf": TFAutoModelForMaskedLM if is_tf_available() else None,
        "pt": AutoModelForMaskedLM if is_torch_available() else None,
        "default": {"model": {"pt": "distilroberta-base", "tf": "distilroberta-base"}},
    },
    "summarization": {
        "impl": SummarizationPipeline,
        "tf": TFAutoModelForSeq2SeqLM if is_tf_available() else None,
        "pt": AutoModelForSeq2SeqLM if is_torch_available() else None,
        "default": {"model": {"pt": "sshleifer/distilbart-cnn-12-6", "tf": "t5-small"}},
    },
    # This task is a special case as it's parametrized by SRC, TGT languages.
    "translation": {
        "impl": TranslationPipeline,
        "tf": TFAutoModelForSeq2SeqLM if is_tf_available() else None,
        "pt": AutoModelForSeq2SeqLM if is_torch_available() else None,
        "default": {
            ("en", "fr"): {"model": {"pt": "t5-base", "tf": "t5-base"}},
            ("en", "de"): {"model": {"pt": "t5-base", "tf": "t5-base"}},
            ("en", "ro"): {"model": {"pt": "t5-base", "tf": "t5-base"}},
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
    "zero-shot-classification": {
        "impl": ZeroShotClassificationPipeline,
        "tf": TFAutoModelForSequenceClassification if is_tf_available() else None,
        "pt": AutoModelForSequenceClassification if is_torch_available() else None,
        "default": {
            "model": {"pt": "facebook/bart-large-mnli", "tf": "roberta-large-mnli"},
            "config": {"pt": "facebook/bart-large-mnli", "tf": "roberta-large-mnli"},
            "tokenizer": {"pt": "facebook/bart-large-mnli", "tf": "roberta-large-mnli"},
        },
    },
    "conversational": {
        "impl": ConversationalPipeline,
        "tf": TFAutoModelForCausalLM if is_tf_available() else None,
        "pt": AutoModelForCausalLM if is_torch_available() else None,
        "default": {
            "model": {
                "pt": "microsoft/DialoGPT-medium",
                "tf": "microsoft/DialoGPT-medium",
            }
        },
    },
    "image-classification": {
        "impl": ImageClassificationPipeline,
        "tf": None,
        "pt": AutoModelForImageClassification if is_torch_available() else None,
        "default": {"model": {"pt": "google/vit-base-patch16-224"}},
    },
}

TASK_ALIASES = {
    "sentiment-analysis": "text-classification",
    "ner": "token-classification",
}


class Huggingface(InDriver):
    def get(
        self,
        task: str,
        model: Optional = None,
        config: Optional[Union[str, PretrainedConfig]] = None,
        tokenizer: Optional[Union[str, PreTrainedTokenizer]] = None,
        framework: Optional[str] = None,
        revision: Optional[str] = None,
        model_kwargs: Dict[str, Any] = {},
        **kwargs,
    ) -> Pipeline:
        """
        Args:
        task (:obj:`str`):
            The task defining which pipeline will be returned. Currently accepted tasks are:
                - :obj:`"feature-extraction"`
                - :obj:`"text-classification"`
                - :obj:`"sentiment-analysis"` (alias of :obj:`"text-classification")
                - :obj:`"token-classification"`
                - :obj:`"ner"` (alias of :obj:`"token-classification")
                - :obj:`"question-answering"`
                - :obj:`"fill-mask"`
                - :obj:`"summarization"`
                - :obj:`"translation_xx_to_yy"`
                - :obj:`"translation"`
                - :obj:`"text-generation"`
                - :obj:`"conversational"`
        model (:obj:`str` or :obj:`~transformers.PreTrainedModel` or :obj:`~transformers.TFPreTrainedModel`, `optional`):
            The model that will be used by the pipeline to make predictions.
        config (:obj:`str` or :obj:`~transformers.PretrainedConfig`, `optional`):
            The configuration that will be used by the pipeline to instantiate the model. This can be a model
            identifier or an actual pretrained model configuration inheriting from
            :class:`~transformers.PretrainedConfig`.
        tokenizer (:obj:`str` or :obj:`~transformers.PreTrainedTokenizer`, `optional`):
            The tokenizer that will be used by the pipeline to encode data for the model. This can be a model
            identifier or an actual pretrained tokenizer inheriting from :class:`~transformers.PreTrainedTokenizer`.
         framework (:obj:`str`, `optional`):
            The framework to use, either :obj:`"pt"` for PyTorch or :obj:`"tf"` for TensorFlow. The specified framework
            must be installed.
         kwargs:
            Additional keyword arguments passed along to the specific pipeline init (see the documentation for the
            corresponding pipeline class for possible values).
        Returns:
            NLP Pipeline
        """
        if task in TASK_ALIASES:
            task = TASK_ALIASES[task]

        targeted_task = TASKS[task]
        task_class = targeted_task["impl"]

        if task not in TASKS:
            raise KeyError(
                "Unknown task {}, available tasks are {}".format(
                    task, list(TASKS.keys())
                )
            )

        if framework is None:
            framework, model = infer_framework_from_model(
                model, targeted_task, revision=revision, task=task
            )

        task_class, model_class = targeted_task["impl"], targeted_task[framework]
        if isinstance(config, str):
            config = AutoConfig.from_pretrained(
                config, revision=revision, _from_pipeline=task, **model_kwargs
            )

        if model is None:
            model = targeted_task["default"]["model"][framework]

        if tokenizer is None:
            if isinstance(model, str):
                tokenizer = model
            else:
                # Impossible to guest what is the right tokenizer here
                raise Exception(
                    "Please provided a PretrainedTokenizer "
                    "class or a path/identifier to a pretrained tokenizer."
                )
        if isinstance(tokenizer, (str, tuple)):
            tokenizer = AutoTokenizer.from_pretrained(tokenizer)

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
                model,
                config=config,
                revision=revision,
                _from_pipeline=task,
                **model_kwargs,
            )

        return task_class(
            model=model, framework=framework, tokenizer=tokenizer, task=task, **kwargs
        )
