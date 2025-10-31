FROM node:18-alpine

# Create app directory
WORKDIR /app

# Install app dependencies
COPY package*.json ./
RUN npm install --production

# Copy app source
COPY . .

# Expose ports for producer and consumer metrics
EXPOSE 3001 3002

# Start both producer and consumer
CMD ["node", "index.js"]