import logging
import os
import subprocess

def deploy(current_configuration, design_path, output, test_id):
    try:
        result = subprocess.run(
            ["docker", "ps", "--no-trunc", "--format", "{{.ID}}"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            logging.error("docker ps failed: %s", result.stderr.strip())
            running_ids = []
        else:
            running_ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
            logging.info(f"Found {len(running_ids)} running container IDs via docker ps.")
            for container_id in running_ids:
                pid_result = subprocess.run(
                    ["docker", "inspect", "-f", "{{.State.Pid}}", container_id],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                process_id = pid_result.stdout.strip() if pid_result.returncode == 0 else "N/A"
                logging.info(f"Container ID: {container_id} PID: {process_id}")


                # HERE


                print(f"{container_id} {process_id}")
    except Exception:
        logging.exception("Could not obtain running container IDs via docker ps.")
        running_ids = []

    if output is not None:
        ids_file = os.path.join(output, "running_container_ids.txt")
        with open(ids_file, "w") as f:
            for container_id in running_ids:
                f.write(f"{container_id}\n")

    current_configuration["docker_running_container_ids"] = " ".join(running_ids)
    return running_ids

def undeploy(current_configuration, design_path, output, test_id):
    pass
