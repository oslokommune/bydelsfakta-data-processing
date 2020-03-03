from dataclasses import dataclass


@dataclass(frozen=True)
class Pipeline:
    id: str
    task_config: dict


@dataclass(frozen=True)
class OutputDataset:
    id: str
    version: str
    edition: str
    s3_prefix: str


@dataclass
class StepData:
    s3_input_prefixes: dict
    status: str
    errors: list

    @property
    def input_count(self):
        return len(self.s3_input_prefixes.items())


@dataclass(frozen=True)
class Payload:
    pipeline: Pipeline
    output_dataset: OutputDataset
    step_data: StepData

    def __init__(self, pipeline: dict, output_dataset: dict, step_data: dict):
        object.__setattr__(self, "pipeline", Pipeline(**pipeline))
        object.__setattr__(self, "output_dataset", OutputDataset(**output_dataset))
        object.__setattr__(self, "step_data", StepData(**step_data))


@dataclass(frozen=True)
class Config:
    execution_name: str
    task: str
    payload: Payload

    @classmethod
    def from_lambda_event(cls, event: dict):
        return Config(
            execution_name=event["execution_name"],
            task=event["task"],
            payload=event["payload"],
        )

    def __init__(self, execution_name, task, payload: dict):
        object.__setattr__(self, "execution_name", execution_name)
        object.__setattr__(self, "task", task)
        object.__setattr__(self, "payload", Payload(**payload))
