import argparse
import itertools
import logging
import sys
from pathlib import Path

from yaml import safe_load

from glassflow import GlassFlowClient
from glassflow import Pipeline as GlassFlowPipeline
from glassflow.utils.yaml_models import Pipeline

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


def load_yaml_file(file):
    """Loads Pipeline YAML file"""
    # Load YAML
    with open(file) as f:
        yaml_data = safe_load(f)

    return Pipeline(**yaml_data)


def yaml_file_to_pipeline(
    yaml_path: Path, personal_access_token: str
) -> GlassFlowPipeline:
    """
    Converts a Pipeline YAML file into GlassFlow SDK Pipeline
    """
    yaml_file_dir = yaml_path.parent
    p = load_yaml_file(yaml_path)

    # We have one source, transformer and sink blocks
    source = [b for b in p.blocks if b.type == "source"][0]
    transformer = [b for b in p.blocks if b.type == "transformer"][0]
    sink = [b for b in p.blocks if b.type == "sink"][0]

    if transformer.requirements is not None:
        if transformer.requirements.value is not None:
            requirements = transformer.requirements.value
        else:
            with open(yaml_file_dir / transformer.requirements.path) as f:
                requirements = f.read()
    else:
        requirements = None

    if transformer.transformation.path is not None:
        transform = str(yaml_file_dir / transformer.transformation.path)
    else:
        transform = str(yaml_file_dir / "handler.py")
        with open(transform, "w") as f:
            f.write(transformer.transformation.value)

    pipeline_id = str(p.pipeline_id) if p.pipeline_id is not None else None
    env_vars = [e.model_dump(exclude_none=True) for e in transformer.env_vars]

    # TODO: Handle source and sink config_secret_ref
    # TODO: Handle env_var value_secret_ref
    return GlassFlowPipeline(
        personal_access_token=personal_access_token,
        id=pipeline_id,
        name=p.name,
        space_id=p.space_id.__str__(),
        env_vars=env_vars,
        transformation_file=transform,
        requirements=requirements,
        sink_kind=sink.kind,
        sink_config=sink.config,
        source_kind=source.kind,
        source_config=source.config,
    )


def get_yaml_files_with_changes(pipelines_dir: Path, files: list[Path]) -> set[Path]:
    """
    Given a list of pipeline file (`.yaml`, `.yml`, `.py` or
    `requirements.txt`) it returns the pipeline YAML files that
    the files belong to.
    """
    pipeline_2_files = map_yaml_to_files(pipelines_dir)

    pipelines_changed = set()
    for file in files:
        if file.suffix in [".yaml", ".yml"]:
            pipelines_changed.add(file)
        elif file.suffix == ".py" or file.name == "requirements.txt":
            for k in pipeline_2_files:
                if file in pipeline_2_files[k]:
                    pipelines_changed.add(k)
        else:
            continue

    return pipelines_changed


def map_yaml_to_files(path: Path) -> dict[Path, list[Path]]:
    """Maps Pipeline YAML files to .py and requirements.txt files"""
    yml_files = itertools.chain(path.rglob("*.yaml"), path.rglob("*.yml"))
    mapping = {}
    for file in yml_files:
        mapping[file] = []
        for b in load_yaml_file(file).blocks:
            if b.type == "transformer":
                transformer = b
                break
        else:
            continue

        if transformer.requirements.path is not None:
            path = file.parent / transformer.requirements.path
            mapping[file].append(path)

        if transformer.transformation.path is not None:
            path = file.parent / transformer.transformation.path
            mapping[file].append(path)
    return mapping


def add_pipeline_id_to_yaml(yaml_path: Path, pipeline_id: str):
    """Prepend the pipeline id to the yaml file"""
    with open(yaml_path, "r+") as f:
        content = f.read()
        f.seek(0, 0)
        f.write(f"pipeline_id: {pipeline_id}" + "\n" + content)


def query_yes_no(question: str, default="yes") -> bool:
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError(f"invalid default answer: '{default}'")

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            log.info("Please respond with 'yes' or 'no' " "(or 'y' or 'n').\n")


def get_changes_summary(
    changed_pipelines: list[GlassFlowPipeline],
    deleted_pipelines: list[GlassFlowPipeline],
):
    """Returns a dictionary of changes that will be applied"""
    to_create = len([p for p in changed_pipelines if p.id is None])
    to_update = len(changed_pipelines) - to_create
    to_update_ids = [p.id for p in changed_pipelines if p.id is not None]
    to_delete = len(deleted_pipelines)
    to_delete_ids = [p.id for p in deleted_pipelines]

    log.info(
        f"""
Expected changes on your GlassFlow pipelines:
\t‣ Create {to_create} pipelines
\t‣ Update {to_update} pipelines {"" if to_update == 0 else f'(IDs: {to_update_ids})'}
\t‣ Delete {to_delete} pipelines {"" if to_update == 0 else f'(IDs: {to_delete_ids})'}
        """
    )
    return {
        "to_create": {"quantity": to_create},
        "to_update": {"quantity": to_update, "ids": to_update_ids},
        "to_delete": {"quantity": to_delete, "ids": to_delete_ids},
    }


def delete_pipelines(files_deleted: list[Path], client: GlassFlowClient):
    for file in files_deleted:
        if file.suffix in [".yaml", ".yml"]:
            p = yaml_file_to_pipeline(
                yaml_path=file, personal_access_token=client.personal_access_token
            )
            p.delete()
            log.info(f"Deleted pipeline {p.id}")


def push_to_cloud(
    files_changed: list[Path],
    files_deleted: list[Path],
    pipelines_dir: Path,
    client: GlassFlowClient,
    auto_approve: bool = False,
):
    if files_deleted is not None:
        deleted_pipelines = [
            yaml_file_to_pipeline(f, client.personal_access_token)
            for f in files_deleted
            if f.suffix in [".yaml", ".yml"]
        ]
    else:
        deleted_pipelines = []

    if files_changed is not None:
        yaml_files_to_update = get_yaml_files_with_changes(
            pipelines_dir=pipelines_dir, files=files_changed
        )
        changed_pipelines = [
            yaml_file_to_pipeline(yaml_file, client.personal_access_token)
            for yaml_file in yaml_files_to_update
        ]
    else:
        yaml_files_to_update = []
        changed_pipelines = []

    get_changes_summary(changed_pipelines, deleted_pipelines)
    if not auto_approve:
        update = query_yes_no("Do you want to proceed?", default="no")
        if not update:
            log.info("Pipelines push cancelled!")
            exit(0)

    for pipeline, yaml_file in zip(changed_pipelines, yaml_files_to_update):
        if pipeline.id is None:
            # Create pipeline
            new_pipeline = pipeline.create()
            add_pipeline_id_to_yaml(yaml_file, new_pipeline.id)
        else:
            # Update pipeline
            existing_pipeline = client.get_pipeline(pipeline.id)
            existing_pipeline.update(
                name=pipeline.name,
                transformation_file=pipeline.transformation_file,
                requirements=pipeline.requirements,
                sink_kind=pipeline.sink_kind,
                sink_config=pipeline.sink_config,
                source_kind=pipeline.source_kind,
                source_config=pipeline.source_config,
                env_vars=pipeline.env_vars,
            )


def main():
    parser = argparse.ArgumentParser("Push pipelines configuration to GlassFlow cloud")
    parser.add_argument(
        "-d",
        "--files-deleted",
        help="List of files that were deleted (`.yaml`, `.yml`, `.py` or "
        "`requirements.txt`) to sync to GlassFlow.",
        type=Path,
        nargs="+",
    )
    parser.add_argument(
        "-a",
        "--files-changed",
        help="List of files with changes (`.yaml`, `.yml`, `.py` or "
        "`requirements.txt`) to sync to GlassFlow.",
        type=Path,
        nargs="+",
    )
    parser.add_argument(
        "--pipelines-dir",
        help="Path to directory with your GlassFlow pipelines.",
        type=Path,
        default="pipelines/",
    )
    parser.add_argument(
        "-t",
        "--personal-access-token",
        help="GlassFlow Personal Access Token.",
        type=str,
    )
    parser.add_argument(
        "-y",
        "--auto-approve",
        action="store_true",
        default=False,
        required=False,
        help="Automatically approve pipelines without prompting for input.",
    )
    args = parser.parse_args()

    client = GlassFlowClient(personal_access_token=args.personal_access_token)
    push_to_cloud(
        files_deleted=args.files_deleted,
        files_changed=args.files_changed,
        pipelines_dir=args.pipelines_dir,
        client=client,
        auto_approve=args.auto_approve,
    )


if __name__ == "__main__":
    main()
