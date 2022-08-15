import asyncio
import io
import json

from PIL import Image
from dataclasses import dataclass
import base64
import aiohttp
import uuid
import os

@dataclass
class UpscaleRequest:
    folder:str
    id:str
    image_b64:str

@dataclass
class StoreRequest:
    folder:str
    id: str
    image_b64: str
    is_upscaled: bool

class DalleMiniApi:
    url = "https://bf.dallemini.ai/generate"

    async def generate_image_async(self, client, description):
        data = {
            "prompt": description
        }

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        }

        print(f"Start scraping for '{description}'")
        async with client.post(self.url, verify_ssl=False, headers=headers, data=json.dumps(data)) as resp:
            response = await resp.text()

            images = json.loads(response)["images"]
            return images

class ZyroApi:

    def __init__(self):
        self.url = "https://upscaler.zyro.com/v1/ai/image-upscaler"

    async def upscale_async(self, client, image):
        data = {
            "image_data": f"data:image/jpeg;base64,{image}"
        }

        headers = {
            "Content-Type": "application/json",
        }

        async with client.post(self.url, verify_ssl=False, headers=headers, data=json.dumps(data)) as resp:
            response = await resp.json()

            image = response["upscaled"][23:]
            return image


def store_generated_image(request: StoreRequest):

    if not os.path.exists("results"):
        os.makedirs("results")

    folder_path = f"results/{request.folder}"

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    data = base64.b64decode(request.image_b64)

    img = Image.open(io.BytesIO(data))
    suffix = "big" if request.is_upscaled else "small"
    img.save(f"{folder_path}/{request.id}_{suffix}.png", 'png')

async def produce_scrape_request_async(queue, phrases, suffixes, count):
    for phrase in phrases:
        for suffix in suffixes:
            text = ", ".join([phrase, suffix])

            for i in range(0, count):
                await queue.put(text)
                print(f"Request for '{text}' has been created")

async def produce_images_async(descriptions_queue, store_queue, upscale_queue):

    api = DalleMiniApi()

    async with aiohttp.ClientSession() as client:
        while True:
            description:str = await descriptions_queue.get()
            folder_name = description.replace(",","").replace(" ", "_")

            print(f"Fetched '{description}' for image generation")

            try:
                images = await api.generate_image_async(client, description)
            except Exception as ex:
                print(f"Was not able to generate images for '{description}': {ex}")
                descriptions_queue.task_done()
                return

            for image in images:

                id = str(uuid.uuid4())

                print(f"Creating saving request for '{id}'")
                await store_queue.put(StoreRequest(
                    image_b64=image,
                    folder=folder_name,
                    is_upscaled=False,
                    id=id
                ))

                print(f"Creating upscaling request for '{id}'")
                await upscale_queue.put(UpscaleRequest(
                    image_b64=image,
                    folder=folder_name,
                    id=id
                ))

            descriptions_queue.task_done()

async def produce_stored_files_async(store_queue):
    while True:
        request:StoreRequest = await store_queue.get()
        print(f"Fetched store request for {request.id} (upscaled: {request.is_upscaled})")

        store_generated_image(request)
        print(f"Done with store request for {request.id} (upscaled: {request.is_upscaled})")
        store_queue.task_done()

async def produce_upscaled_images(upscale_queue, store_queue):

    api = ZyroApi()

    async with aiohttp.ClientSession() as client:
        while True:
            request: UpscaleRequest = await upscale_queue.get()

            print(f"Fetched upscale request for image {request.id}")

            try:
                upscaled = await api.upscale_async(client, request.image_b64)
                await store_queue.put(StoreRequest(
                    id=request.id,
                    folder=request.folder,
                    is_upscaled=True,
                    image_b64=upscaled
                ))

                print(f"Done with upscaling for image {request.id}")
            except Exception as ex:
                print(f"Was NOT able to upscale image {request.id}: {ex}")

            upscale_queue.task_done()

async def run(phrases, suffixes, amount):
    dallemini_workers_count = 5 # don't send too many simultaneous requests to the external api
    upscaler_workers_count = 5 # don't send too many simultaneous requests to the external api


    descriptions_queue = asyncio.Queue(maxsize=dallemini_workers_count)
    upscale_thumbnails_queue = asyncio.Queue(maxsize=upscaler_workers_count)
    store_thumbnails_queue = asyncio.Queue()

    generator_tasks = [asyncio.create_task(produce_images_async(descriptions_queue, store_thumbnails_queue, upscale_thumbnails_queue))
                       for _ in range(0, dallemini_workers_count)]
    upscaler_tasks = [asyncio.create_task(produce_upscaled_images(upscale_thumbnails_queue, store_thumbnails_queue))
                      for _ in range(0, upscaler_workers_count)]
    saving_task = asyncio.create_task(produce_stored_files_async(store_thumbnails_queue))

    await produce_scrape_request_async(descriptions_queue, phrases, suffixes, amount)
    await descriptions_queue.join()
    await upscale_thumbnails_queue.join()
    await store_thumbnails_queue.join()


if __name__ == "__main__":
    import platform

    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(run(["..."], ["..."], 5))
