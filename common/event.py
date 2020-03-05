from dataclasses import asdict
from functools import wraps

from dataplatform.pipeline.s3 import Config
from dataplatform.awslambda.logging import log_add

from common.aws import read_from_s3, find_s3_key


class EventHandlerError(Exception):
    pass


class PrefixNotInInputError(EventHandlerError):
    def __init__(self, prefixes_not_found):
        self.prefixes_not_found = prefixes_not_found


class ExtraPrefixesInInputError(EventHandlerError):
    def __init__(self, extra_prefixes):
        self.extra_prefixes = extra_prefixes


def event_handler(status="OK", **kwargs):
    """Decorate a processor function to handle Lambda events

    Parses the Lambda event and fetches data from S3 and supplies it to the
    decorated function.

    Wrapped function must accept `output_prefix` and `type_of_ds` arguments, in
    addition to any number of dataframe arguments given to this decorator as
    arguments.

    Ie. given `@event_handler(foo_df="foo", bar_df="bar")` the decorated
    function must accept `foo_df` and `bar_df` arguments.

    ```
    @event_handler(my_df="key-to-s3-prefix")
    def process(my_df, output_prefix, type_of_ds):
        pass
    ```
    """

    def decorator(fn):
        @wraps(fn)
        def wrapper(event, context):
            config = Config.from_lambda_event(event)
            task = config.task
            output_dataset = config.payload.output_dataset
            s3_input_prefixes = config.payload.step_data.s3_input_prefixes

            task_config = config.payload.pipeline.task_config[task]

            log_add(
                dataset_id=output_dataset.id,
                version=output_dataset.version,
                edition_id=output_dataset.edition,
                task=task,
                task_config=str(task_config),
                source_key=str(config.payload.step_data.s3_input_prefixes),
            )

            type_of_ds = task_config["type"]
            output_prefix = (
                output_dataset.s3_prefix.replace("%stage%", "intermediate") + task + "/"
            )

            prefixes = {}
            not_found = []
            extra_prefixes = list(s3_input_prefixes.keys())
            for arg, s3_key in kwargs.items():
                if s3_key in s3_input_prefixes:
                    prefixes[arg] = s3_input_prefixes[s3_key]
                    extra_prefixes.remove(s3_key)
                else:
                    not_found.append(s3_key)

            if not_found:
                raise PrefixNotInInputError(not_found)
            if extra_prefixes:
                raise ExtraPrefixesInInputError(extra_prefixes)

            df_kwargs = {
                arg: read_from_s3(find_s3_key(prefix))
                for arg, prefix in prefixes.items()
            }

            fn(**df_kwargs, output_prefix=output_prefix, type_of_ds=type_of_ds)

            output = asdict(config.payload.step_data)
            output["s3_input_prefixes"] = {
                config.payload.output_dataset.id: output_prefix
            }
            output["status"] = status
            return output

        return wrapper

    return decorator
