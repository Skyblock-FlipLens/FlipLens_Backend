package com.skyblockflipper.backend.NEU;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class NEUClient {
    private final Git git;
    private String hash;

    public NEUClient(@Value("${config.NEU.repo-url}") String url) throws GitAPIException {
        git = Git.cloneRepository().setURI(url).call();
    }

    public void fetchJSONs(){

    }

    public void hasUpdates(){

    }
}
