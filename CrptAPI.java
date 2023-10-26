package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class CrptAPI implements AutoCloseable {

    private final RequestSender requestSender;
    private final DocumentPublisher publisher;

    public CrptAPI(TimeUnit timeUnit, int requestLimit) {
        requestSender = new RequestSender(timeUnit, requestLimit);
        publisher = new DocumentPublisher();
    }

    public String createDocument(Document doc, String signature, String productGroup) {

        CrptRequest<String> request = new PublicationRequest(publisher, signature, doc, productGroup);
        requestSender.send(request);

        request.awaitResult();

        try {
            return request.getResult().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws InterruptedException {
        requestSender.close();
    }

    public interface CrptRequest<T> extends Callable<T> {
        Future<T> getResult();

        void setResult(Future<T> result);

        void awaitResult();
    }

    public static class PublicationRequest implements CrptRequest<String> {

        private final DocumentPublisher publisher;
        private final String signature;
        private final String productGroup;
        private final Document doc;
        private Future<String> result;

        public PublicationRequest(DocumentPublisher publisher, String signature, Document doc, String productGroup) {
            this.publisher = publisher;
            this.signature = signature;
            this.doc = doc;
            this.productGroup = productGroup;
        }

        @Override
        public synchronized void awaitResult() {
            while (getResult() == null) ;
        }

        @Override
        public synchronized Future<String> getResult() {
            return result;
        }

        @Override
        public synchronized void setResult(Future<String> result) {
            this.result = result;
        }

        @Override
        public String call() throws Exception {
            String token = publisher.authorize(signature);
            return publisher.publish(token, signature, doc, productGroup);
        }
    }

    public static class DocumentPublisher {

        private final HttpClient httpClient;

        public DocumentPublisher() {
            httpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .build();
        }

        public String authorize(String signature) throws IOException, InterruptedException {

            HttpRequest signatureRequest = HttpRequest.newBuilder()
                    .uri(URI.create("https://ismp.crpt.ru/api/v3/auth/cert/key"))
                    .GET()
                    .build();

            HttpResponse<String> signatureResponse = httpClient.send(signatureRequest, HttpResponse.BodyHandlers.ofString());
            if (signatureResponse.statusCode() != 200) {
                throw new RuntimeException("Signature request failed with status code: " + signatureResponse.statusCode());
            }

            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(signatureResponse.body());
            ((ObjectNode) node).put("data", signature);

            HttpRequest tokenRequest = HttpRequest.newBuilder()
                    .uri(URI.create("https://ismp.crpt.ru/api/v3/auth/cert"))
                    .POST(HttpRequest.BodyPublishers.ofString(node.toString()))
                    .build();

            HttpResponse<String> tokenResponse = httpClient.send(tokenRequest, HttpResponse.BodyHandlers.ofString());
            if (tokenResponse.statusCode() != 200) {
                throw new RuntimeException("Token request failed with status code: " + tokenResponse.statusCode());
            }

            return mapper.readTree(tokenResponse.body()).get("token").toString();
        }

        public String publish(String token, String signature, Document doc, String productGroup) throws IOException, InterruptedException {

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://ismp.crpt.ru/api/v3/lk/docs/commissioning/contract/create?pg=" + productGroup))
                    .header("Authorization", String.format("Bearer %s", token))
                    .POST(HttpRequest.BodyPublishers.ofString(
                            String.format(
                                    "{\"product_document\": %s, \"document_format\": %s, \"type\": %s, \"signature\": %s}",
                                    doc, doc.getType().getFormat(), doc.getType(), signature)))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(response.body());
            if (node.get("code") != null) {
                throw new RuntimeException(String.format("%s: %s", node.get("error_message"), node.get("description")));
            }

            return node.get("value").toString();
        }
    }

    public static class RequestSender implements AutoCloseable {

        private final RateLimiter limiter;
        private final TimeUnit timeUnit;
        private final ExecutorService executorService;
        private final BlockingQueue<CrptRequest<?>> queuedRequests = new LinkedBlockingQueue<>();

        public RequestSender(TimeUnit timeUnit, long requestLimit) {
            this.limiter = new RateLimiter(timeUnit, requestLimit);
            this.timeUnit = timeUnit;
            this.executorService = Executors.newFixedThreadPool(requestLimit > 20 ? 20 : (int) requestLimit);
            Runnable queueControllerThread = () -> {
                while (true) {
                    try {
                        while (limiter.tryAcquire() && !queuedRequests.isEmpty()) {
                            CrptRequest request = queuedRequests.poll();
                            Future<?> future = executorService.submit(request);
                            request.setResult(future);
                        }
                        Thread.sleep(timeUnit.toMillis(1));
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            };
            executorService.execute(queueControllerThread);
        }

        public <T> void send(CrptRequest<T> request) {
            if (!limiter.tryAcquire()) {
                queuedRequests.offer(request);
            }

            request.setResult(executorService.submit(request));
        }

        @Override
        public void close() throws InterruptedException {
            executorService.shutdown();
            if (executorService.awaitTermination(1, timeUnit)) {
                executorService.shutdownNow();
            }
        }
    }

    public static class RateLimiter {

        private final long capacity;
        private final AtomicLong tokens;
        private final long ratePerMillis;
        private final AtomicLong lastRefillTime;

        public RateLimiter(TimeUnit timeUnit, long requestLimit) {
            this.capacity = requestLimit;
            this.tokens = new AtomicLong(requestLimit);
            this.ratePerMillis = requestLimit * 1000 / timeUnit.toMillis(1);
            this.lastRefillTime = new AtomicLong(System.currentTimeMillis());
        }

        public boolean tryAcquire() {
            refill();
            return tokens.getAndUpdate(tokens -> tokens > 0 ? tokens - 1 : tokens) > 0;
        }

        private void refill() {
            long currentTime = System.currentTimeMillis();
            long elapsedTime = currentTime - lastRefillTime.get();
            long tokensToAdd = elapsedTime * ratePerMillis / 1000;
            if (tokensToAdd > 0) {
                lastRefillTime.set(currentTime);
                tokens.updateAndGet(tokens -> Math.min(capacity, tokens + tokensToAdd));
            }
        }
    }

    public static class Document {

        private final DocumentType type;
        private final String content;

        public Document(DocumentType type, String content) {
            this.type = type;
            this.content = content;
        }

        public DocumentType getType() {
            return type;
        }

        public String getContent() {
            return content;
        }

        public enum DocumentType {
            LP_INTRODUCE_GOODS,
            LP_INTRODUCE_GOODS_CSV,
            LP_INTRODUCE_GOODS_XML;

            public String getFormat() {
                switch (this) {
                    case LP_INTRODUCE_GOODS:
                        return "MANUAL";
                    case LP_INTRODUCE_GOODS_CSV:
                        return "CSV";
                    case LP_INTRODUCE_GOODS_XML:
                        return "XML";
                    default:
                        return null;
                }
            }
        }
    }
}
