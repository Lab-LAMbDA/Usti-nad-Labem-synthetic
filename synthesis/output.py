import os, datetime, json

def configure(context):
    for option in ("output_path", "sampling_rate", "random_seed"):
        context.config(option)

def validate(context):
    output_path = context.config("output_path")

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)

def execute(context):
    output_path = context.config("output_path")

    print("Writing meta information")

    # Write meta information
    information = dict(
        sampling_rate = context.config("sampling_rate"),
        random_seed = context.config("random_seed"),
        created = datetime.datetime.now(datetime.timezone.utc).isoformat()
    )

    with open("%s/meta.json" % output_path, "w+") as f:
        json.dump(information, f, indent = 4)
