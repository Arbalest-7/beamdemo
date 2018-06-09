package demo;

/**
 * Created by mawu on 2018/5/27.
 */

class Egg2 {
    class Yolk{
        public Yolk (){
            System.out.println("Egg2.yolk");
        }
        public void f() {
            System.out.println("Egg2.yolk.f");
        }
    }
    private Yolk yolk = new Yolk();

    public Egg2() {
        System.out.println("new Egg2");
    }
    public void insert(Yolk yolk) {
        yolk = yolk;
    }
    public void g() {
        yolk.f();
    }
}
public class BigEgg2 extends Egg2{

    class Yolk extends Egg2.Yolk {
        public Yolk() {
            System.out.println("BigEgg.Yoklk2");
        }

        @Override
        public void f() {
            System.out.println("yolk2");
        }
    }
    public BigEgg2() {
        insert(new Yolk());
    }

    public static void main(String[] args) {
         BigEgg2 bigEgg2 = new BigEgg2();
         bigEgg2.g();
    }
}
