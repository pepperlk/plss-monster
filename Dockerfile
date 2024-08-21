FROM pepperlk/dev-gis
# install geopandas dependencies
# RUN apt-get update && apt-get install -y \
#     python3-geopandas \
#     python3-numpy \
#     python3-gdal \
#     && rm -rf /var/lib/apt/lists/*



# # # install python dependencies
# RUN pip install requests folium matplotlib mapclassify pyscaffold

# # insatll ipywidgets for jupyter notebook and kernel
# RUN pip install ipywidgets ipykernel

# # copy the local arcgis_helpers package to the container

# install gdal
RUN apt-get update && apt-get install -y \
    git \
    gdal-bin \
    libgdal-dev \
    python3-gdal \
    && rm -rf /var/lib/apt/lists/*



WORKDIR /app
CMD ["/bin/bash"]