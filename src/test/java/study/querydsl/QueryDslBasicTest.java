package study.querydsl;

import com.querydsl.core.QueryResults;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.Team;

import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static study.querydsl.entity.QMember.*;

@SpringBootTest
@Transactional
public class QueryDslBasicTest {

    @PersistenceContext
    EntityManager em;

    JPAQueryFactory queryFactory;

    @BeforeEach
    public void before() {

        queryFactory = new JPAQueryFactory(em);

        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    @DisplayName("JPQL로 조회")
    public void startJPQL() throws Exception {
        // member1을 찾아라.
        Member findMember = em.createQuery("select m from Member m where m.userName = :userName", Member.class)
                .setParameter("userName", "member1")
                .getSingleResult();

        assertThat(findMember.getUserName()).isEqualTo("member1");
    }

    @Test
    @DisplayName("QueryDsl로 조회")
    public void startQueryDsl() throws Exception {

        Member findMember = queryFactory.select(member)
                .from(member)
                .where(member.userName.eq("member1"))
                .fetchOne();

        assertThat(findMember.getUserName()).isEqualTo("member1");

    }

    @Test
    @DisplayName("검색 조건 쿼리")
    public void search() throws Exception {

        Member findMember = queryFactory.selectFrom(member)
                .where(member.userName.eq("member1").and(member.age.eq(10)))
                .fetchOne();

        assertThat(findMember.getUserName()).isEqualTo("member1");
    }

    @Test
    @DisplayName("검색 조건 쿼리 - 가독성")
    public void searchAndParam() throws Exception {

        Member findMember = queryFactory.selectFrom(member)
                .where(
                        member.userName.eq("member1"),
                        member.age.eq(10)
                )
                .fetchOne();

        assertThat(findMember.getUserName()).isEqualTo("member1");
    }

    @Test
    public void resultFetch() {
        //List
        List<Member> fetch = queryFactory
                .selectFrom(member)
                .fetch();
        //단 건
        Member findMember1 = queryFactory
                .selectFrom(member)
                .fetchOne();
        //처음 한 건 조회
        Member findMember2 = queryFactory
                .selectFrom(member)
                .fetchFirst();
        //페이징에서 사용
        // queryDsl 5.0 부터 fetchResults()가 deprecated 됨
        QueryResults<Member> results = queryFactory
                .selectFrom(member)
                .fetchResults();
        //count 쿼리로 변경
        // queryDsl 5.0 부터 fetchCount()가 deprecated 됨
        long count = queryFactory
                .selectFrom(member)
                .fetchCount();
    }


}
