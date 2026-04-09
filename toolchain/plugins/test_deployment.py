import logging
import docker

def ready(current_configuration, design_path, output, test_id):
    container_names_to_check = current_configuration["test_if_image_is_present"].split()

    try:
        client = docker.from_env()
        running_containers = client.containers.list()
    except Exception as e:
        logging.critical(f"Cannot connect to Docker: {repr(e)}")
        return False

    running_names = []
    for container in running_containers:
        running_names.append(container.name)
        running_names.extend(container.attrs.get("Names", []))

    for container_name in container_names_to_check:
        if not any(container_name in name for name in running_names):
            logging.critical(f"No running container contains '{container_name}' in its name.")
            return False

    return True
