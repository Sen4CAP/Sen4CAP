FROM almalinux:9

LABEL name='r-classification' \
    maintainer='Laurentiu Nicola <laurentiu.nicola@c-s.ro>'

RUN dnf -y install epel-release && \
    crb enable && \
    dnf -y install R libcurl-devel libxml2-devel openssl-devel harfbuzz-devel fribidi-devel freetype-devel libpng-devel libtiff-devel libjpeg-devel && \
    echo -e "packages <- c('arrow', 'e1071', 'caret', 'dplyr', 'gsubfn', 'ranger', 'readr', 'smotefamily', 'caTools', 'tidyverse', 'data.table')\n\
        diff <- setdiff(packages, rownames(installed.packages()))\n\
        if (length(diff) > 0) {\n\
            install.packages(diff, Ncpus = $(nproc), repos = c(CRAN = 'https://cran.rstudio.com'))\n\
        }" | LIBARROW_BINARY=true Rscript - && \
    dnf -y clean all

COPY crop_type.R /usr/bin
