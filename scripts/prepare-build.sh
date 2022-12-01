yum install -y epel-release || sudo yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1).noarch.rpm
yum install -y https://apache.jfrog.io/artifactory/arrow/centos/$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1)/apache-arrow-release-latest.rpm
yum install -y --enablerepo=epel arrow-devel # For C++
yum install -y --enablerepo=epel arrow-glib-devel # For GLib (C)
yum install -y --enablerepo=epel arrow-dataset-devel # For Apache Arrow Dataset C++
yum install -y --enablerepo=epel arrow-dataset-glib-devel # For Apache Arrow Dataset GLib (C)
yum install -y --enablerepo=epel parquet-devel # For Apache Parquet C++
yum install -y --enablerepo=epel parquet-glib-devel # For Apache Parquet GLib (C)
