import os

# Configura umask 0022 global para garantir que todos os arquivos e diretórios
# criados pela aplicação tenham permissões de leitura pública (0644 / 0755),
# permitindo que outros containers (como o Apache Hop) leiam os arquivos
# compartilhados em volumes sem conflito de permissão POSIX.
os.umask(0o022)
