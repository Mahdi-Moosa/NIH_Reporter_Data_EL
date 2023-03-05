# import asyncio
# import aiohttp
# import gzip

# async def download_file(session: aiohttp.ClientSession, url: str):
#     # Get the file name from the url
#     file_name = url.split("/")[-1]
#     # Send a GET request to the url
#     async with session.get(url) as response:
#         # Check if the response is successful
#         print(f'Request URL is: {url}')
#         print(f'Status of response was {response.status}')
#         assert response.status == 200
#         # Read the response content as bytes
#         data = await response.read()
#         # Open a gzip file for writing in binary mode
#         with gzip.open(f'data/{file_name}', "wb") as f:
#             # Write the data to the file
#             f.write(data)
#         # Print a message when done
#         print(f"Downloaded {file_name}")

# async def main():
#     # The base url for downloading gz files
#     base_url = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
#     # A list of gz file names to download (you can modify this as needed)
#     gz_files = ["pubmed23n0001.xml.gz", "pubmed23n0002.xml.gz", "pubmed23n0003.xml.gz"]
#     # Create an aiohttp session object
#     async with aiohttp.ClientSession() as session:
#         # Create a list of tasks for downloading each file asynchronously
#         tasks = [asyncio.create_task(download_file(session, base_url + file)) for file in gz_files]
#         # Wait for all tasks to finish
#         await asyncio.gather(*tasks)

# # Run the main function using asyncio event loop
# asyncio.run(main())

# ------------------------------------------------------------------------------------------------------------------------------
# With semaphore (i.e., limiting number of requests sent; ref: https://rednafi.github.io/reflections/limit-concurrency-with-semaphore-in-python-asyncio.html)

import asyncio
import aiohttp
import gzip

async def download_file(url, sem):
    # Get the file name from the url
    file_name = url.split("/")[-1]
    async with sem: # acquire semaphore
        print(f"Acquired semaphore for {url}")
        async with aiohttp.ClientSession() as session:
            print(f"Making request for {url}")
            async with session.get(url) as response:
                data = await response.read()
                # do something with data
                # Open a gzip file for writing in binary mode
                with gzip.open(f'data/{file_name}', "wb") as f:
                    # Write the data to the file
                    f.write(data)
                # Print a message when done
                print(f"Downloaded {file_name}")
                # print(f"Downloaded {url}")
        # release semaphore
        print(f"Released semaphore for {url}")

async def main():
    sem = asyncio.Semaphore(3) # create semaphore with limit 3
    base_url = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
    tasks = []
    for i in range(1, 11): # create 10 tasks
        url = f"{base_url}pubmed23n{i:04d}.xml.gz"
        task = asyncio.create_task(download_file(url, sem))
        tasks.append(task)
    await asyncio.gather(*tasks)

asyncio.run(main())