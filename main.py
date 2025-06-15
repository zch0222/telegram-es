import uvicorn
from fastapi import FastAPI
from core.config import PORT, HOST

app = FastAPI()



if __name__ == "__main__":
 
    uvicorn.run("main:app", host=HOST, port=int(PORT), reload=True)

