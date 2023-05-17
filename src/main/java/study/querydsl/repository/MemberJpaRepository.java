package study.querydsl.repository;

import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Repository;
import study.querydsl.entity.Member;

import java.util.List;
import java.util.Optional;

import static study.querydsl.entity.QMember.*;

@Repository
public class MemberJpaRepository {

    private final EntityManager em;
    private final JPAQueryFactory queryFactory;

    public MemberJpaRepository(EntityManager em) {
        this.em = em;
        this.queryFactory = new JPAQueryFactory(em);
    }

    public void save(Member member) {
        em.persist(member);
    }

    public Optional<Member> findById(Long id) {
        Member findMember = em.find(Member.class, id);
        return Optional.ofNullable(findMember);
    }

    public List<Member> findAll() {
        return em.createQuery("select m from Member m", Member.class)
                .getResultList();

    }

    public List<Member> findAll_Querydsl() {
        return queryFactory.selectFrom(member).fetch();
    }



    public List<Member> findByUserName(String userName) {
        return em.createQuery("select m from Member m where m.userName = :userName")
                .setParameter("userName", userName)
                .getResultList();
    }


    public List<Member> findByUserName_Querydsl(String userName) {
        return queryFactory.selectFrom(member)
                .where(member.userName.eq(userName))
                .fetch();
    }

}
