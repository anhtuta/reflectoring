package hello.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class User {
    private String name;

    public User(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "User [name=" + name + "]";
    }
}
