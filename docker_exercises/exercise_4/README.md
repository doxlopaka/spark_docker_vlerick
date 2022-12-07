# Data Minded Academy - Containerization with Docker
## Exercise 4 - Write the Dockerfile of a simple Python application

In this exercise, you are asked to containerize a simple Streamlit app helping you to visualize typical 
neural network activation functions in an interactive environment.

The source code of the application is available in the subfolder `app` of the `exercise_4` folder. 

You are asked to containerize this simple application. To do so, refer to the application's `README.md`. 

1. Write the Dockerfile that will define your Docker image, build, and run it to make sure everything is working as expected.

2. Try to reflect on how to optimize your Dockerfile in terms of layers and caching capability. Make 
sure the Python dependencies installation isn't re-run at build time every time something changed in `streamlit_app.py`.

3. What's the current size of your Docker image? Can you do something to reduce it?

4. [Create an account in DockerHub](https://hub.docker.com/) and push the Streamlit app image to it. 
Delete the image you have locally and try to pull it from your Dockerhub account.