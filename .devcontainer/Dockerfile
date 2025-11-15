FROM maven:3.9.2-eclipse-temurin-17
# Add non-root user

ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl vim wget build-essential zsh locales \
    && locale-gen en_US.UTF-8 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
# RUN useradd -ms /bin/zsh devuser && usermod -aG sudo devuser

# Install Oh My Zsh and plugins
RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended \
    && git clone https://github.com/zsh-users/zsh-autosuggestions ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/plugins/zsh-autosuggestions \
    && git clone https://github.com/zsh-users/zsh-syntax-highlighting ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/plugins/zsh-syntax-highlighting \ 
    && sed -i 's/ZSH_THEME=".*"/ZSH_THEME="simple"/' $HOME/.zshrc \
    && sed -i 's/plugins=(git)/plugins=(git zsh-autosuggestions zsh-syntax-highlighting)/' ~/.zshrc

SHELL ["/bin/zsh", "-c"]

WORKDIR /workspace

# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
# ENV PATH=$JAVA_HOME/bin:$PATH

# RUN mvn clean package assembly:single -DskipTests


CMD ["zsh"]