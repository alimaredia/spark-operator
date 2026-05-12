# Dockerfile for Apache Spark (OpenShift Compatible)
# Uses official Apache Spark image and makes it compatible with OpenShift's arbitrary UID requirements
FROM apache/spark:4.0.1

LABEL description="Apache Spark image compatible with OpenShift restricted-v2 SCC"

# --- OpenShift Arbitrary UID Compatibility ---
# OpenShift assigns arbitrary non-root UID at runtime, but it's always a member of GID 0
# All directories must be owned by group 0 and group-writable (g=u)
USER root

# Set Spark directories to be owned by group 0 and group-writable
# Required for: reading jars, writing to work-dir/logs
RUN chgrp -R 0 /opt/spark && \
    chmod -R g=u /opt/spark && \
    mkdir -p /opt/spark/work-dir /opt/spark/logs && \
    chgrp -R 0 /opt/spark/work-dir /opt/spark/logs && \
    chmod -R 775 /opt/spark/work-dir /opt/spark/logs

# Ensure /tmp is writable (usually is, but explicit for verification)
RUN chmod 1777 /tmp

# Set HOME for Spark temp files
ENV HOME=/home/spark

# Create HOME directory with proper permissions
RUN mkdir -p /home/spark && \
    chgrp -R 0 /home/spark && \
    chmod -R g=u /home/spark && \
    chmod 775 /home/spark
