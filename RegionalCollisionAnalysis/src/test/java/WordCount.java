import java.io.Serializable;

/**
 * @ClassName WordCount
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/18
 */

public class WordCount implements Serializable {
    public int number;
    public String word;
    public int frequency;

    public WordCount(int number, String word, int frequency) {
        this.number = number;
        this.word = word;
        this.frequency = frequency;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }
}