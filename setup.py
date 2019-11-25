import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="spatial-pipeline-sensors", # Replace with your own username
    version="0.0.1",
    author="scapulomancy",
    description="Spatial Data Pipeline with some aggregations functions, useful to aggregate data around sensors",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/scapulomancy/SensorDataPipelines",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.7.5",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)