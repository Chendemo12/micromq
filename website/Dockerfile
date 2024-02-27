FROM node:alpine3.18 as build
WORKDIR /app

COPY package.json .
RUN npm install
COPY . /app
RUN npm run build-only


FROM nginx:stable-alpine3.17
COPY --from=build /app/dist /usr/share/nginx/html
RUN rm /etc/nginx/conf.d/default.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
