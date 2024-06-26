import java.util.*;
import java.lang.*;
import java.io.*;

class Eratosthenes {
    public static void main(String[] args) throws java.lang.Exception {
    	
    	// Ghi lại thời gian bắt đầu
        long startTime = System.nanoTime();
        
        int N = 100000000;
        boolean[] check = new boolean[N + 1];
        
        // Khởi tạo tất cả các số [2...N] đều là số nguyên tố
        for (int i = 2; i <= N; i++) {
            check[i] = true;
        }

        // Thuật toán sàng nguyên tố
        // Nếu một số là số nguyên tố, thì tất cả các bội của nó không phải số nguyên tố
        for (int i = 2; i <= N; i++) {
            if (check[i] == true) {
                for (int j = 2 * i; j <= N; j += i) {
                    check[j] = false;
                }
            }
        }

        // In ra các số là số nguyên tố
        for (int i = 2; i <= N; i++) {
            if (check[i] == true) {
                System.out.print(i + " ");
            }
        }
        // Ghi lại thời gian kết thúc
        long endTime = System.nanoTime();
        
        // Tính và in ra thời gian thực thi
        long elapsedTime = endTime - startTime;
        System.out.println("\nThời gian thực thi: " + elapsedTime + " ns");
    }
}
