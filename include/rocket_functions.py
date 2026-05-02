import os
import requests


def fetch_launches():
    """
    Fetch upcoming rocket launches from Launch Library API.
    Returns a list of launch records.
    """
    url = "https://ll.thespacedevs.com/2.2.0/launch/upcoming/?limit=5"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()["results"]


def download_images(launches):
    """
    Download rocket images into a temporary directory inside the Airflow container.
    Returns summary stats.
    """
    image_dir = "/tmp/rocket_images"
    os.makedirs(image_dir, exist_ok=True)

    download_count = 0

    for launch in launches:
        image_url = launch.get("image")
        if not image_url:
            continue

        file_name = image_url.split("/")[-1]
        target_path = os.path.join(image_dir, file_name)

        # Avoid downloading duplicates
        if not os.path.exists(target_path):
            img_res = requests.get(image_url, timeout=30)
            img_res.raise_for_status()

            with open(target_path, "wb") as f:
                f.write(img_res.content)

            download_count += 1

    return {
        "total_processed": len(launches),
        "new_downloads": download_count
    }