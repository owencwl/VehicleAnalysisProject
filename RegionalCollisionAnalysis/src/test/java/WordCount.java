import java.io.Serializable;

/**
 * @ClassName WordCount
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/18
 */

public  class WordCount implements Serializable {
    public Integer number;
    public String word;
    public Integer frequency;

    public WordCount(Integer number, String word, Integer frequency) {
        this.number = number;
        this.word = word;
        this.frequency = frequency;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(Integer number) {
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

    public void setFrequency(Integer frequency) {
        this.frequency = frequency;
    }
}