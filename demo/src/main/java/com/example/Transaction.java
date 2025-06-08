package com.example;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Transaction {

    private String transactionId;
    private int merchantId;
    private String product;
    private int quantity;
    private int customerId;
    private double amount;
    private String transactionTime;
    private String paymentType;

    

    private static final Random random = new Random();

    private static final LocalDateTime now = LocalDateTime.now();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String FORMATTED_DATE = now.format(formatter);

    @Override
    public String toString() {
        return "Transaction: {"+
            "transactionID:'" + transactionId +
            ", merchantID:1" + merchantId +
            ", product:'" + product +
            ", qtty:" + quantity +
            ", customerId: 2" + customerId +
            ", amount:" + amount +
            ", transactionTime:" + transactionTime +
            ", paymentType:'" + paymentType +
            '}';
    }



    public static Transaction randomTrans() {
        Transaction trans = new Transaction();
        List<String> newList = new ArrayList<>();
        newList.add("iphone");
        newList.add("acer predator");
        newList.add("sony dslr");
        newList.add("secret lab gaming chair");
        int index = random.nextInt(newList.size());

        trans.setTransactionId(UUID.randomUUID().toString());
        trans.setMerchantId(1 + random.nextInt(100));
        trans.setProduct(newList.get(index));
        trans.setCustomerId(2 + random.nextInt(1000));
        trans.setQuantity(random.nextInt(1, 5));
        trans.setAmount(120 + (495 * (random.nextDouble())));
        trans.setTransactionTime(FORMATTED_DATE);
        trans.setPaymentType(random.nextBoolean() ? "credit_card": "digital_wallet");
        return trans;
    }

    public static void main(String[] args) {
        System.out.println(Transaction.randomTrans());
    }
}